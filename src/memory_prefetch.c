/*
 * This file utilizes prefetching keys and data for multiple commands in a batch,
 * to improve performance by amortizing memory access costs across multiple operations.
 *
 * Copyright (c) 2025-Present, Redis Ltd. and contributors.
 * All rights reserved.
 *
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#include "memory_prefetch.h"
#include "server.h"
#include "hashtable.h"

typedef enum {
    PREFETCH_ENTRY,      /* Use hashtable incremental find to prefetch buckets/entries */
    PREFETCH_KVOBJ,      /* Prefetch the kvobj once entry is found */
    PREFETCH_VALDATA,    /* Prefetch the value data of the kvobj */
    PREFETCH_DONE        /* Indicates that prefetching for this key is complete */
} PrefetchState;


/************************************ State machine diagram for the prefetch operation. ********************************
                                                           │
                                                         start
                                                           │
                                                  ┌────────▼─────────┐
                                                  │  PREFETCH_ENTRY  │────────────────┐
                                                  └────────┬─────────┘           not found -> done
                                                     entry │ found                    │
                                                           │                          │
                                                  ┌────────▼────────┐                 │
                                                  │ PREFETCH_KVOBJ  │                 │
                                                  └────────┬────────┘                 │
                                                           │                          │
                                               ┌───────────▼────────────┐             │
                                               │   PREFETCH_VALDATA     │             │
                                               └───────────┬────────────┘             │
                                                           │                          │
                                                 ┌─────────▼─────────────┐            │
                                                 │     PREFETCH_DONE     │◄───────────┘
                                                 └───────────────────────┘
**********************************************************************************************************************/

typedef void *(*GetValueDataFunc)(const void *val);

typedef struct KeyPrefetchInfo {
    PrefetchState state;                           /* Current state of the prefetch operation */
    hashtableIncrementalFindState find_state;      /* Hashtable incremental find state */
    kvobj *current_kv;                             /* Pointer to the kv object being prefetched */
} KeyPrefetchInfo;

/* PrefetchCommandsBatch structure holds the state of the current batch of client commands being processed. */
typedef struct PrefetchCommandsBatch {
    size_t cur_idx;                 /* Index of the current key being processed */
    size_t key_count;               /* Number of keys in the current batch */
    size_t client_count;            /* Number of clients in the current batch */
    size_t pcmd_count;              /* Number of pending commands in the current batch */
    size_t max_prefetch_size;       /* Maximum number of keys to prefetch in a batch */
    void **keys;                    /* Array of keys to prefetch in the current batch */
    client **clients;               /* Array of clients in the current batch */
    pendingCommand **pending_cmds;  /* Array of pending commands in the current batch */
    hashtable **keys_hts;           /* Main hashtable for each key */
    KeyPrefetchInfo *prefetch_info; /* Prefetch info for each key */
    GetValueDataFunc get_value_data_func; /* Function to get the value data */
} PrefetchCommandsBatch;

static PrefetchCommandsBatch *batch = NULL;

void freePrefetchCommandsBatch(void) {
    if (batch == NULL) {
        return;
    }

    zfree(batch->clients);
    zfree(batch->pending_cmds);
    zfree(batch->keys);
    zfree(batch->keys_hts);
    zfree(batch->prefetch_info);
    zfree(batch);
    batch = NULL;
}

void prefetchCommandsBatchInit(void) {
    serverAssert(!batch);

    /* To avoid prefetching small batches, we set the max size to twice
     * the configured size, so if not exceeding twice the limit, we can
     * prefetch all of it. See also `determinePrefetchCount` */
    size_t max_prefetch_size = server.prefetch_batch_max_size * 2;

    if (max_prefetch_size == 0) {
        return;
    }

    batch = zcalloc(sizeof(PrefetchCommandsBatch));
    batch->max_prefetch_size = max_prefetch_size;
    batch->clients = zcalloc(max_prefetch_size * sizeof(client *));
    batch->pending_cmds = zcalloc(max_prefetch_size * sizeof(pendingCommand *));
    batch->keys = zcalloc(max_prefetch_size * sizeof(void *));
    batch->keys_hts = zcalloc(max_prefetch_size * sizeof(hashtable *));
    batch->prefetch_info = zcalloc(max_prefetch_size * sizeof(KeyPrefetchInfo));
}

void onMaxBatchSizeChange(void) {
    if (batch && batch->client_count > 0) {
        /* We need to process the current batch before updating the size */
        return;
    }

    freePrefetchCommandsBatch();
    prefetchCommandsBatchInit();
}

/* Advance the batch cursor to the next key. */
static inline void moveToNextKey(void) {
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

/* Prefetch the given pointer and move to the next key in the batch. */
static inline void prefetchAndMoveToNextKey(void *addr) {
    redis_prefetch_read(addr);
    moveToNextKey();
}

static inline void markKeyAsdone(KeyPrefetchInfo *info) {
    info->state = PREFETCH_DONE;
    server.stat_total_prefetch_entries++;
}

/* Returns the next KeyPrefetchInfo structure that needs to be processed. */
static KeyPrefetchInfo *getNextPrefetchInfo(void) {
    size_t start_idx = batch->cur_idx;
    do {
        KeyPrefetchInfo *info = &batch->prefetch_info[batch->cur_idx];
        if (info->state != PREFETCH_DONE) return info;
        batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
    } while (batch->cur_idx != start_idx);
    return NULL;
}

static void initBatchInfo(GetValueDataFunc func) {
    batch->get_value_data_func = func;

    /* Initialize the prefetch info using hashtable's incremental find API */
    for (size_t i = 0; i < batch->key_count; i++) {
        KeyPrefetchInfo *info = &batch->prefetch_info[i];
        hashtable *ht = batch->keys_hts[i];
        if (!ht || hashtableSize(ht) == 0) {
            info->state = PREFETCH_DONE;
            continue;
        }
        info->current_kv = NULL;
        info->state = PREFETCH_ENTRY;
        /* Initialize incremental find - this computes the hash and sets up the state */
        hashtableIncrementalFindInit(&info->find_state, ht, batch->keys[i]);
    }
}

/* Use hashtable's incremental find to prefetch entry data.
 * This uses the hashtable's built-in prefetching which handles buckets and entries. */
static void prefetchEntry(KeyPrefetchInfo *info) {
    /* Step the incremental find - this prefetches the next bucket/entry */
    if (hashtableIncrementalFindStep(&info->find_state)) {
        /* More work to do - continue in PREFETCH_ENTRY state.
         * The incremental find step already issued its own prefetch,
         * so just advance to the next key. */
        moveToNextKey();
        return;
    }

    /* Incremental find complete - get the result */
    void *entry;
    if (hashtableIncrementalFindGetResult(&info->find_state, &entry)) {
        /* Entry found - prefetch the kvobj */
        info->current_kv = entry;
        redis_prefetch_read(info->current_kv);
        info->state = PREFETCH_KVOBJ;
    } else {
        /* Entry not found */
        markKeyAsdone(info);
    }
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

/* Prefetch kvobj fields needed for the next stage.
 * The kvobj IS the value object (they're the same thing - redisObject). */
static inline void prefetchKVObject(KeyPrefetchInfo *info) {
    kvobj *kv = info->current_kv;
    if (kv) {
        /* The kvobj contains ptr which points to the actual data.
         * For strings with RAW encoding, ptr points to the sds string.
         * Prefetch the data pointer. */
        if (kv->ptr) {
            prefetchAndMoveToNextKey(kv->ptr);
            info->state = PREFETCH_VALDATA;
            return;
        }
    }
    markKeyAsdone(info);
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

/* Prefetch the actual value data (e.g., string content) */
static void prefetchValueData(KeyPrefetchInfo *info) {
    kvobj *kv = info->current_kv;
    if (kv && batch->get_value_data_func) {
        /* get_value_data_func returns the actual data to prefetch
         * (e.g., for RAW strings, the sds string content) */
        void *data = batch->get_value_data_func(kv);
        if (data) {
            redis_prefetch_read(data);
        }
    }
    markKeyAsdone(info);
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

/* Prefetch hashtable data for an array of keys.
 *
 * This function takes an array of hashtables and keys, attempting to bring
 * data closer to the L1 cache that might be needed for hashtable lookup
 * operations on those keys.
 *
 * The prefetch algorithm uses hashtable's incremental find API:
 * 1. Initialize incremental find for each key (computes hash)
 * 2. Step through the find process, prefetching buckets and entries
 * 3. Once entry is found, prefetch the kvobj and its value data
 *
 * hashtablePrefetch executes incrementally, one step at a time for each key.
 * Instead of waiting for data to be read from memory, it prefetches
 * the data and then moves on to execute the next prefetch for another key.
 *
 * get_val_data_func - A callback function that hashtablePrefetch can invoke
 * to bring the key's value data closer to the L1 cache as well.
 */
static void hashtablePrefetch(GetValueDataFunc get_val_data_func) {
    initBatchInfo(get_val_data_func);
    KeyPrefetchInfo *info;
    while ((info = getNextPrefetchInfo())) {
        switch (info->state) {
        case PREFETCH_ENTRY: prefetchEntry(info); break;
        case PREFETCH_KVOBJ: prefetchKVObject(info); break;
        case PREFETCH_VALDATA: prefetchValueData(info); break;
        default: serverPanic("Unknown prefetch state %d", info->state);
        }
    }
}

/* Helper function to get the value data pointer from an robj.
 * Used for prefetching the actual string data. */
static void *getObjectValuePtr(const void *value) {
    const robj *o = value;
    return (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_RAW) ? o->ptr : NULL;
}

/* Intra-command prefetch: stack-allocated variant of hashtablePrefetch() for
 * use within a single multi-key command (e.g. MGET/MSET). */
void hashtablePrefetchKeys(hashtable **hts, void **keys, size_t nkeys) {
    if (nkeys <= 1) return;
    serverAssert(nkeys <= HASHTABLE_PREFETCH_MAX_SIZE);
    server.stat_total_prefetch_batches++;

    KeyPrefetchInfo infos[HASHTABLE_PREFETCH_MAX_SIZE];
    size_t pending = 0;

    for (size_t i = 0; i < nkeys; i++) {
        if (!hts[i] || hashtableSize(hts[i]) == 0) {
            infos[i].state = PREFETCH_DONE;
            continue;
        }
        infos[i].state = PREFETCH_ENTRY;
        infos[i].current_kv = NULL;
        hashtableIncrementalFindInit(&infos[i].find_state, hts[i], keys[i]);
        pending++;
    }

    size_t idx = 0;
    while (pending > 0) {
        KeyPrefetchInfo *info = &infos[idx];
        if (info->state == PREFETCH_DONE) {
            idx = (idx + 1) % nkeys;
            continue;
        }
        switch (info->state) {
        case PREFETCH_ENTRY:
            if (hashtableIncrementalFindStep(&info->find_state)) break;
            void *entry;
            if (hashtableIncrementalFindGetResult(&info->find_state, &entry)) {
                info->current_kv = entry;
                redis_prefetch_read(info->current_kv);
                info->state = PREFETCH_KVOBJ;
            } else {
                info->state = PREFETCH_DONE;
                server.stat_total_prefetch_entries++;
                pending--;
            }
            break;
        case PREFETCH_KVOBJ:
            if (info->current_kv && info->current_kv->ptr) {
                redis_prefetch_read(info->current_kv->ptr);
                info->state = PREFETCH_VALDATA;
            } else {
                info->state = PREFETCH_DONE;
                server.stat_total_prefetch_entries++;
                pending--;
            }
            break;
        case PREFETCH_VALDATA: {
            void *data = getObjectValuePtr(info->current_kv);
            if (data) redis_prefetch_read(data);
            info->state = PREFETCH_DONE;
            server.stat_total_prefetch_entries++;
            pending--;
            break;
        }
        default:
            serverPanic("Unknown prefetch state %d", info->state);
        }
        idx = (idx + 1) % nkeys;
    }
}

void resetCommandsBatch(void) {
    if (batch == NULL) {
        /* Handle the case where prefetching becomes enabled from disabled. */
        if (server.prefetch_batch_max_size) prefetchCommandsBatchInit();
        return;
    }

    batch->cur_idx = 0;
    batch->key_count = 0;
    batch->client_count = 0;
    batch->pcmd_count = 0;

    /* Handle the case where the max prefetch size has been changed. */
    if (batch->max_prefetch_size != (size_t)server.prefetch_batch_max_size * 2) {
        onMaxBatchSizeChange();
    }
}

/* Prefetching in very small batches tends to be ineffective because the technique
 * relies on a small gap—typically a few CPU cycles—between issuing the prefetch
 * and performing the actual memory access. If the batch is too small, this delay
 * cannot be effectively inserted, and the prefetching yields little to no benefit.
 *
 * To avoid wasting effort, when the remaining data is small (less than twice the
 * maximum batch size), we simply prefetch all of it at once. Otherwise, we only
 * prefetch a limited portion, capped at the configured maximum. */
int determinePrefetchCount(int len) {
    if (!batch) return 0;

    /* The batch max size is double of the configured size. */
    int config_size = batch->max_prefetch_size / 2;
    return len < (int)batch->max_prefetch_size ? len : config_size;
}

/* Prefetch command-related data:
 * 1. Prefetch the command arguments allocated by the I/O thread to bring them
 *    closer to the L1 cache.
 * 2. Prefetch the io_deferred_objects for all clients.
 * 3. Prefetch the keys and values for all commands in the current batch from
 *    the main dictionaries. */
void prefetchCommands(void) {
    if (!batch || server.loading) return;

    /* Prefetch argv's for all pending commands */
    for (size_t i = 0; i < batch->pcmd_count; i++) {
        pendingCommand *pcmd = batch->pending_cmds[i];
        if (unlikely(pcmd->argc <= 0)) continue;
        for (int j = 0; j < pcmd->argc; j++) {
            redis_prefetch_read(pcmd->argv[j]);
        }
    }

    /* Prefetch the argv->ptr if required */
    for (size_t i = 0; i < batch->pcmd_count; i++) {
        pendingCommand *pcmd = batch->pending_cmds[i];
        if (unlikely(pcmd->argc <= 1)) continue;
        /* Skip the first argument (command name), as it's typically short */
        for (int j = 1; j < pcmd->argc; j++) {
            if (pcmd->argv[j]->encoding == OBJ_ENCODING_RAW) {
                redis_prefetch_read(pcmd->argv[j]->ptr);
            }
        }
    }

    /* Prefetch io_deferred_objects for all clients */
    for (size_t i = 0; i < batch->client_count; i++) {
        client *c = batch->clients[i];
        if (!c->io_deferred_objects || c->io_deferred_objects_num == 0) continue;
        for (int j = 0; j < c->io_deferred_objects_num; j++)
            redis_prefetch_read(c->io_deferred_objects[j]);
    }

    /* Get the keys ptrs - we do it here after the key obj was prefetched. */
    for (size_t i = 0; i < batch->key_count; i++) {
        batch->keys[i] = ((robj *)batch->keys[i])->ptr;
    }

    /* Prefetch keys for all commands.
     * Prefetching is beneficial only if there are more than one key. */
    if (batch->key_count > 1) {
        server.stat_total_prefetch_batches++;
        /* Prefetch keys from the main hashtable */
        hashtablePrefetch(getObjectValuePtr);
    }
}

/* Adds the client's command to the current batch.
 *
 * Returns C_OK if the command was added successfully, C_ERR otherwise. */
int addCommandToBatch(client *c) {
    if (unlikely(!batch)) return C_ERR;

    /* If the batch is full, process it.
     * We also check the client count to handle cases where
     * no keys exist for the clients' commands. */
    if (batch->client_count == batch->max_prefetch_size ||
        batch->key_count == batch->max_prefetch_size ||
        batch->pcmd_count == batch->max_prefetch_size)
    {
        return C_ERR;
    }

    /* Avoid partial prefetching: if the batch already has commands and adding this
     * client's ready commands would likely exceed the batch size limit, reject
     * the entire client. This is a conservative estimate using command count as
     * a proxy for key count to ensure all keys from a client are either fully
     * prefetched together or not prefetched at all. */
    if (batch->pcmd_count > 0 &&
        (c->pending_cmds.ready_len + batch->key_count > batch->max_prefetch_size ||
         c->pending_cmds.ready_len + batch->pcmd_count > batch->max_prefetch_size))
    {
        return C_ERR;
    }

    batch->clients[batch->client_count++] = c;

    pendingCommand *pcmd = c->pending_cmds.head;
    while (pcmd != NULL && batch->pcmd_count < batch->max_prefetch_size) {
        if (pcmd->next) redis_prefetch_read(pcmd->next);

        /* Skip commands that have not been preprocessed, or have errors. */
        if ((pcmd->flags & PENDING_CMD_FLAG_INCOMPLETE) || !pcmd->cmd || pcmd->read_error) break;

        batch->pending_cmds[batch->pcmd_count++] = pcmd;

        serverAssert(pcmd->flags & PENDING_CMD_KEYS_RESULT_VALID);
        hashtable *cmd_ht = kvstoreGetDict(c->db->keys, pcmd->slot > 0 ? pcmd->slot : 0);
        for (int i = 0; i < pcmd->keys_result.numkeys && batch->key_count < batch->max_prefetch_size; i++) {
            batch->keys[batch->key_count] = pcmd->argv[pcmd->keys_result.keys[i].pos];
            batch->keys_hts[batch->key_count] = cmd_ht;
            batch->key_count++;
            /* Mark the command as prefetched so the intra-command prefetch
             * path skips it. Even on a partial batch, running both paths
             * would just contend for cache bandwidth. */
            pcmd->flags |= PENDING_CMD_KEYS_PREFETCHED;
        }
        pcmd = pcmd->next;
    }

    return C_OK;
}
