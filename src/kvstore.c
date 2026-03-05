/*
 * Copyright (c) 2011-Present, Redis Ltd. and contributors.
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

#include "fmacros.h"

#include <string.h>
#include <stddef.h>

#include "zmalloc.h"
#include "kvstore.h"
#include "fwtree.h"
#include "redisassert.h"
#include "monotonic.h"

#define UNUSED(V) ((void) V)

/* Random number generation for fair random dict selection */
#include "mt19937-64.h"
#define randomULong() ((unsigned long)genrand64_int64())

/* Default dict metadata size when caller doesn't provide dictMetadataBytes */
static size_t kvstoreDictMetaBaseSize(void) {
    return sizeof(kvstoreDictMetaBase);
}

struct _kvstore {
    int flags;
    kvstoreType *type;
    hashtableType htype;
    hashtable **dicts;
    long long num_dicts;
    long long num_dicts_bits;
    list *rehashing;                       /* List of dictionaries in this kvstore that are currently rehashing. */
    int resize_cursor;                     /* Cron job uses this cursor to gradually resize dictionaries (only used if num_dicts > 1). */
    int allocated_dicts;                   /* The number of allocated dicts. */
    int non_empty_dicts;                   /* The number of non-empty dicts. */
    unsigned long long key_count;          /* Total number of keys in this kvstore. */
    unsigned long long bucket_count;       /* Total number of buckets in this kvstore across dictionaries. */
    fenwickTree *dict_sizes;               /* Binary indexed tree (BIT) that describes cumulative key frequencies up until given dict-index. */
    size_t overhead_hashtable_rehashing;   /* The overhead of dictionaries rehashing. */
    void *metadata[];                      /* conditionally allocated based on "flags" */
};

/**********************************/
/*** Helpers **********************/
/**********************************/

/* Get the dictionary pointer based on dict-index. */
hashtable *kvstoreGetDict(kvstore *kvs, int didx) {
    return kvs->dicts[didx];
}

static hashtable **kvstoreGetDictRef(kvstore *kvs, int didx) {
    return &kvs->dicts[didx];
}

static int kvstoreDictIsRehashingPaused(kvstore *kvs, int didx)
{
    hashtable *ht = kvstoreGetDict(kvs, didx);
    return ht ? hashtableIsRehashingPaused(ht) : 0;
}

static void addDictIndexToCursor(kvstore *kvs, int didx, unsigned long long *cursor) {
    if (kvs->num_dicts == 1)
        return;
    /* didx can be -1 when iteration is over and there are no more dicts to visit. */
    if (didx < 0)
        return;
    *cursor = (*cursor << kvs->num_dicts_bits) | didx;
}

static int getAndClearDictIndexFromCursor(kvstore *kvs, unsigned long long *cursor) {
    if (kvs->num_dicts == 1)
        return 0;
    int didx = (int) (*cursor & (kvs->num_dicts-1));
    *cursor = *cursor >> kvs->num_dicts_bits;
    return didx;
}

/* Updates binary index tree (Fenwick tree), updates key count for a given dict */
static void cumulativeKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvs->key_count += delta;

    hashtable *ht = kvstoreGetDict(kvs, didx);
    size_t dsize = hashtableSize(ht);
    /* Increment if dsize is 1 and delta is positive (first element inserted, dict becomes non-empty).
     * Decrement if dsize is 0 (dict becomes empty). */
    int non_empty_dicts_delta = (dsize == 1 && delta > 0) ? 1 : (dsize == 0) ? -1 : 0;
    kvs->non_empty_dicts += non_empty_dicts_delta;

    /* BIT does not need to be calculated when there's only one dict. */
    if (kvs->num_dicts == 1)
        return;

    /* Update the BIT */
    fwTreeUpdate(kvs->dict_sizes, didx, delta);
}

/* Create the dict if it does not exist and return it. */
static hashtable *createDictIfNeeded(kvstore *kvs, int didx) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (ht) return ht;

    kvs->dicts[didx] = hashtableCreate(&kvs->htype);
    kvstoreDictMetaBase *metadata = hashtableMetadata(kvs->dicts[didx]);
    metadata->rehashing_node = NULL;
    /* Note: bucket_count is updated by trackMemUsage callback when bucket
     * tables are allocated (during first insert), not during hashtableCreate. */
    kvs->allocated_dicts++;
    return kvs->dicts[didx];
}

/* Called when the dict will delete entries, the function will check
 * KVSTORE_FREE_EMPTY_DICTS to determine whether the empty dict needs
 * to be freed.
 *
 * Note that for rehashing dicts, that is, in the case of safe iterators
 * and Scan, we won't delete the dict. We will check whether it needs
 * to be deleted when we're releasing the iterator. */
static void freeDictIfNeeded(kvstore *kvs, int didx) {
    if (!(kvs->flags & KVSTORE_FREE_EMPTY_DICTS) ||
        !kvstoreGetDict(kvs, didx) ||
        kvstoreDictSize(kvs, didx) != 0 ||
        kvstoreDictIsRehashingPaused(kvs, didx))
        return;

    /* Use callback if provided to check if dict can be freed */
    if (kvs->type->canFreeDict && !kvs->type->canFreeDict(kvs, didx))
        return;

    hashtableRelease(kvs->dicts[didx]);
    kvs->dicts[didx] = NULL;
    kvs->allocated_dicts--;
}

void kvstoreFreeDictIfNeeded(kvstore *kvs, int didx) {
    freeDictIfNeeded(kvs, didx);
}

/**********************************/
/*** hashtable callbacks **********/
/**********************************/

/* Adds dictionary to the rehashing list, which allows us
 * to quickly find rehash targets during incremental rehashing.
 *
 * Note: bucket_count is tracked via kvstoreDictTrackMemUsage callback, so we
 * don't need to update it here. We only track the rehashing overhead. */
static void kvstoreDictRehashingStarted(hashtable *ht) {
    kvstore *kvs = hashtableGetType(ht)->userdata;
    kvstoreDictMetaBase *metadata = hashtableMetadata(ht);
    listAddNodeTail(kvs->rehashing, ht);
    metadata->rehashing_node = listLast(kvs->rehashing);

    size_t from, to;
    hashtableRehashingInfo(ht, &from, &to);
    /* Note: bucket_count is updated by trackMemUsage callback, not here */
    kvs->overhead_hashtable_rehashing += from * HASHTABLE_BUCKET_SIZE;
}

/* Remove dictionary from the rehashing list.
 *
 * Note: bucket_count is tracked via kvstoreDictTrackMemUsage callback, so we
 * don't need to update it here. We only track the rehashing overhead. */
static void kvstoreDictRehashingCompleted(hashtable *ht) {
    kvstore *kvs = hashtableGetType(ht)->userdata;
    kvstoreDictMetaBase *metadata = hashtableMetadata(ht);
    if (metadata->rehashing_node) {
        listDelNode(kvs->rehashing, metadata->rehashing_node);
        metadata->rehashing_node = NULL;
    }

    size_t from, to;
    hashtableRehashingInfo(ht, &from, &to);
    /* Note: bucket_count is updated by trackMemUsage callback, not here */
    kvs->overhead_hashtable_rehashing -= from * HASHTABLE_BUCKET_SIZE;
}

/* Track memory usage using this callback. It is called with a positive
 * number when the hashtable allocates memory and with a negative number
 * when freeing. We only update bucket_count when the delta is for bucket
 * table allocations (i.e., a multiple of HASHTABLE_BUCKET_SIZE).
 * The hashtable struct allocation and metadata are NOT included in bucket_count. */
static void kvstoreDictTrackMemUsage(hashtable *ht, ssize_t delta) {
    kvstore *kvs = hashtableGetType(ht)->userdata;
    if (kvs == NULL) {
        /* This is the initial allocation by hashtableCreate, when userdata
         * hasn't been set yet. */
        return;
    }
    /* Only count bucket table allocations. Bucket tables are always a multiple
     * of HASHTABLE_BUCKET_SIZE. The hashtable struct allocation is not. */
    if (delta % HASHTABLE_BUCKET_SIZE == 0) {
        kvs->bucket_count += delta / HASHTABLE_BUCKET_SIZE;
    }
}

/**********************************/
/*** API **************************/
/**********************************/

/* Create an array of dictionaries
 * num_dicts_bits is the log2 of the amount of dictionaries needed (e.g. 0 for 1 dict,
 * 3 for 8 dicts, etc.) */
kvstore *kvstoreCreate(kvstoreType *type, hashtableType *dtype, int num_dicts_bits, int flags) {
    /* We can't support more than 2^16 dicts because we want to save 48 bits
     * for the dict cursor, see kvstoreScan */
    assert(num_dicts_bits <= 16);
    assert(!type->dictMetadataBytes || type->dictMetadataBytes() >= sizeof(kvstoreDictMetaBase));

    /* Calc kvstore size */   
    size_t kvsize = sizeof(kvstore);
    /* Conditionally calc also histogram size */
    if (type->kvstoreMetadataBytes)
        kvsize += type->kvstoreMetadataBytes(NULL);
    
    kvstore *kvs = zcalloc(kvsize);
    memcpy(&kvs->htype, dtype, sizeof(kvs->htype));
    kvs->flags = flags;
    kvs->type = type;

    /* kvstore must be the one to set these callbacks, so we make sure the
     * caller didn't do it */
    assert(!dtype->getMetadataSize);
    assert(!dtype->rehashingStarted);
    assert(!dtype->rehashingCompleted);
    assert(!dtype->trackMemUsage);
    assert(!dtype->userdata);
    kvs->htype.getMetadataSize = type->dictMetadataBytes ?
        type->dictMetadataBytes : kvstoreDictMetaBaseSize;
    kvs->htype.rehashingStarted = kvstoreDictRehashingStarted;
    kvs->htype.rehashingCompleted = kvstoreDictRehashingCompleted;
    kvs->htype.trackMemUsage = kvstoreDictTrackMemUsage;
    kvs->htype.userdata = kvs;

    kvs->num_dicts_bits = num_dicts_bits;
    kvs->num_dicts = 1 << kvs->num_dicts_bits;
    kvs->dicts = zcalloc(sizeof(hashtable*) * kvs->num_dicts);
    if (!(kvs->flags & KVSTORE_ALLOCATE_DICTS_ON_DEMAND)) {
        for (int i = 0; i < kvs->num_dicts; i++)
            createDictIfNeeded(kvs, i);
    }

    kvs->rehashing = listCreate();
    kvs->key_count = 0;
    kvs->non_empty_dicts = 0;
    kvs->resize_cursor = 0;
    kvs->dict_sizes = kvs->num_dicts > 1 ? fwTreeCreate(kvs->num_dicts_bits) : NULL;
    kvs->bucket_count = 0;
    kvs->overhead_hashtable_rehashing = 0;
    return kvs;
}

void kvstoreEmpty(kvstore *kvs, void(callback)(hashtable*)) {
    for (int didx = 0; didx < kvs->num_dicts; didx++) {
        hashtable *ht = kvstoreGetDict(kvs, didx);
        if (!ht)
            continue;
        kvstoreDictMetaBase *metadata = hashtableMetadata(ht);
        if (metadata->rehashing_node)
            metadata->rehashing_node = NULL;
        hashtableEmpty(ht, callback);
        if (kvs->type->onDictEmpty) kvs->type->onDictEmpty(kvs, didx);
        freeDictIfNeeded(kvs, didx);
    }

    if (kvs->type->onKvstoreEmpty) kvs->type->onKvstoreEmpty(kvs);

    listEmpty(kvs->rehashing);

    kvs->key_count = 0;
    kvs->non_empty_dicts = 0;
    kvs->resize_cursor = 0;
    kvs->bucket_count = 0;
    if (kvs->dict_sizes)
        fwTreeClear(kvs->dict_sizes);
    kvs->overhead_hashtable_rehashing = 0;
}

void kvstoreRelease(kvstore *kvs) {
    for (int didx = 0; didx < kvs->num_dicts; didx++) {
        hashtable *ht = kvstoreGetDict(kvs, didx);
        if (!ht)
            continue;
        kvstoreDictMetaBase *metadata = hashtableMetadata(ht);
        if (metadata->rehashing_node)
            metadata->rehashing_node = NULL;
        if (kvs->type->onDictEmpty) kvs->type->onDictEmpty(kvs, didx);
        hashtableRelease(ht);
    }
    zfree(kvs->dicts);

    listRelease(kvs->rehashing);
    if (kvs->dict_sizes)
        fwTreeDestroy(kvs->dict_sizes);

    zfree(kvs);
}

unsigned long long int kvstoreSize(kvstore *kvs) {
    return kvs->key_count;
}

/* This method provides the cumulative sum of all the dictionary buckets
 * across dictionaries in a database. */
unsigned long kvstoreBuckets(kvstore *kvs) {
    if (kvs->num_dicts != 1) {
        return kvs->bucket_count;
    } else {
        hashtable *ht = kvs->dicts[0];
        if (!ht) return 0;
        return hashtableBuckets(ht)
             + hashtableChainedBuckets(ht, 0)
             + hashtableChainedBuckets(ht, 1);
    }
}

size_t kvstoreMemUsage(kvstore *kvs) {
    size_t mem = sizeof(*kvs);

    /* Per-dict hashtable struct + metadata overhead */
    size_t metaSize = kvs->htype.getMetadataSize ? kvs->htype.getMetadataSize() : 0;
    mem += kvs->allocated_dicts * (hashtableHeaderSize() + metaSize);

    /* Bucket tables */
    mem += kvs->bucket_count * HASHTABLE_BUCKET_SIZE;

    /* Values are hashtable* shared with kvs->dicts */
    mem += listLength(kvs->rehashing) * sizeof(listNode);

    return mem;
}

/*
 * This method is used to iterate over the elements of the entire kvstore specifically across dicts.
 * It's a three pronged approach.
 *
 * 1. It uses the provided cursor `cursor` to retrieve the dict index from it.
 * 2. If the dictionary is in a valid state checked through the provided callback `hashtableScanValidFunction`,
 *    it performs a hashtableScan over the appropriate `keyType` dictionary of `db`.
 * 3. If the dict is entirely scanned i.e. the cursor has reached 0, the next non empty dict is discovered.
 *    The dict information is embedded into the cursor and returned.
 *
 * To restrict the scan to a single dict, pass a valid dict index as
 * 'onlydidx', otherwise pass -1.
 */
unsigned long long kvstoreScan(kvstore *kvs, unsigned long long cursor,
                               int onlydidx, hashtableScanFunction scan_cb,
                               kvstoreScanShouldSkipDict *skip_cb,
                               void *privdata)
{
    unsigned long long _cursor = 0;
    /* During dictionary traversal, 48 upper bits in the cursor are used for positioning in the HT.
     * Following lower bits are used for the dict index number, ranging from 0 to 2^num_dicts_bits-1.
     * Dict index is always 0 at the start of iteration and can be incremented only if there are
     * multiple dicts. */
    int didx = getAndClearDictIndexFromCursor(kvs, &cursor);
    if (onlydidx >= 0) {
        if (didx < onlydidx) {
            /* Fast-forward to onlydidx. */
            assert(onlydidx < kvs->num_dicts);
            didx = onlydidx;
            cursor = 0;
        } else if (didx > onlydidx) {
            /* The cursor is already past onlydidx. */
            return 0;
        }
    }

    hashtable *ht = kvstoreGetDict(kvs, didx);

    int skip = !ht || (skip_cb && skip_cb(ht, didx));
    if (!skip) {
        _cursor = hashtableScan(ht, cursor, scan_cb, privdata);
        /* In hashtableScan, scan_cb may delete entries (e.g., in active expire case). */
        freeDictIfNeeded(kvs, didx);
    }
    /* scanning done for the current dictionary or if the scanning wasn't possible, move to the next dict index. */
    if (_cursor == 0 || skip) {
        if (onlydidx >= 0)
            return 0;
        didx = kvstoreGetNextNonEmptyDictIndex(kvs, didx);
    }
    if (didx == -1) {
        return 0;
    }
    addDictIndexToCursor(kvs, didx, &_cursor);
    return _cursor;
}

/*
 * This functions increases size of kvstore to match desired number.
 * It resizes all individual dictionaries, unless skip_cb indicates otherwise.
 *
 * Based on the parameter `try_expand`, appropriate hashtable expand API is invoked.
 * if try_expand is set to 1, `hashtableTryExpand` is used else `hashtableExpand`.
 * The return code is either true/false for both the API(s).
 * true response is for successful expansion. However, false response signifies failure in allocation in
 * `hashtableTryExpand` call and in case of `hashtableExpand` call it signifies no expansion was performed.
 */
int kvstoreExpand(kvstore *kvs, uint64_t newsize, int try_expand, kvstoreExpandShouldSkipDictIndex *skip_cb) {
    for (int i = 0; i < kvs->num_dicts; i++) {
        if (skip_cb && skip_cb(i)) continue;
        hashtable *ht = createDictIfNeeded(kvs, i);
        if (!ht) continue;

        int result = try_expand ? hashtableTryExpand(ht, newsize) : hashtableExpand(ht, newsize);
        if (try_expand && !result)
            return 0;
    }

    return 1;
}

/* Returns fair random dict index, probability of each dict being returned is
 * proportional to the number of elements that dictionary holds.
 * This function guarantees that it returns a dict-index of a non-empty dict,
 * unless the entire kvstore is empty or all dicts are skipped.
 *
 * Parameters:
 * - kvs: the kvstore instance
 * - skip_cb: callback to determine if a dict should be skipped (NULL means no skipping)
 * - fair_attempts: number of fair selection attempts before falling back
 * - slow_fallback: if 1, uses systematic search when fair attempts fail
 *
 * Returns:
 * - Valid dict index (>= 0) on success
 * - -1 if no valid dict found (either slow_fallback is 0 or all dicts are skipped)
 *
 * Time complexity: O(fair_attempts * log(kvs->num_dicts)) for fair attempts,
 * plus O(kvs->num_dicts) for systematic fallback if enabled.
 */
int kvstoreGetFairRandomDictIndex(kvstore *kvs, kvstoreRandomShouldSkipDictIndex *skip_cb,
                                  int fair_attempts, int slow_fallback)
{
    if (kvs->num_dicts == 1 || kvstoreSize(kvs) == 0)
        return 0;

    unsigned long long total_size = kvstoreSize(kvs);

    /* Try fair attempts first. If skip_cb is not applicable, execute only once. */
    for (int attempt = 0; attempt < fair_attempts; attempt++) {
        unsigned long target = (randomULong() % total_size) + 1;
        int didx = kvstoreFindDictIndexByKeyIndex(kvs, target);
        if (!skip_cb || !skip_cb(didx)) {
            return didx;
        }
    }

    /* If fair attempts failed and slow fallback is allowed */
    if (slow_fallback) {
        /* systematic check from random start */
        int start = randomULong() % kvs->num_dicts;
        for (int i = 0; i < kvs->num_dicts; i++) {
            int didx = (start + i) % kvs->num_dicts;
            hashtable *ht = kvstoreGetDict(kvs, didx);
            if (ht && (!skip_cb || !skip_cb(didx))) {
                return didx;
            }
        }
    }

    /* Failed to find valid dict that has elements */
    return -1;
}

void kvstoreGetStats(kvstore *kvs, char *buf, size_t bufsize, int full) {
    buf[0] = '\0';

    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;
    hashtableStats *mainHtStats = NULL;
    hashtableStats *rehashHtStats = NULL;
    hashtable *ht;
    kvstoreIterator kvs_it;

    kvstoreIteratorInit(&kvs_it, kvs);
    while ((ht = kvstoreIteratorNextDict(&kvs_it))) {
        hashtableStats *stats = hashtableGetStatsHt(ht, 0, full);
        if (!mainHtStats) {
            mainHtStats = stats;
        } else {
            hashtableCombineStats(stats, mainHtStats);
            hashtableFreeStats(stats);
        }
        if (hashtableIsRehashing(ht)) {
            stats = hashtableGetStatsHt(ht, 1, full);
            if (!rehashHtStats) {
                rehashHtStats = stats;
            } else {
                hashtableCombineStats(stats, rehashHtStats);
                hashtableFreeStats(stats);
            }
        }
    }
    kvstoreIteratorReset(&kvs_it);

    if (mainHtStats && bufsize > 0) {
        l = hashtableGetStatsMsg(buf, bufsize, mainHtStats, full);
        hashtableFreeStats(mainHtStats);
        buf += l;
        bufsize -= l;
    }

    if (rehashHtStats && bufsize > 0) {
        l = hashtableGetStatsMsg(buf, bufsize, rehashHtStats, full);
        hashtableFreeStats(rehashHtStats);
        buf += l;
        bufsize -= l;
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize - 1] = '\0';
}

/* Finds a dict containing target element in a key space ordered by dict index.
 * Consider this example. Dictionaries are represented by brackets and keys by dots:
 *  #0   #1   #2     #3    #4
 * [..][....][...][.......][.]
 *                    ^
 *                 target
 *
 * In this case dict #3 contains key that we are trying to find.
 *
 * The return value is 0 based dict-index, and the range of the target is [1..kvstoreSize], kvstoreSize inclusive.
 *
 * To find the dict, we start with the root node of the binary index tree and search through its children
 * from the highest index (2^num_dicts_bits in our case) to the lowest index. At each node, we check if the target
 * value is greater than the node's value. If it is, we remove the node's value from the target and recursively
 * search for the new target using the current node as the parent.
 * Time complexity of this function is O(log(kvs->num_dicts))
 */
int kvstoreFindDictIndexByKeyIndex(kvstore *kvs, unsigned long target) {
    if (kvs->num_dicts == 1 || kvstoreSize(kvs) == 0)
        return 0;
    assert(target <= kvstoreSize(kvs));

    return fwTreeFindIndex(kvs->dict_sizes, target);
}

/* Get the first non-empty dict index in the kvstore. Returns -1 if kvstore is empty. */
int kvstoreGetFirstNonEmptyDictIndex(kvstore *kvs) {
    if (kvstoreSize(kvs) == 0)
        return -1;
    if (kvs->num_dicts == 1)
        return 0;
    return fwTreeFindFirstNonEmpty(kvs->dict_sizes);
}

/* Returns next non-empty dict index strictly after given one, or -1 if provided didx is the last one. */
int kvstoreGetNextNonEmptyDictIndex(kvstore *kvs, int didx) {
    if (kvs->num_dicts == 1) {
        assert(didx == 0);
        return -1;
    }
    return fwTreeFindNextNonEmpty(kvs->dict_sizes, didx);
}

int kvstoreNumNonEmptyDicts(kvstore *kvs) {
    return kvs->non_empty_dicts;
}

int kvstoreNumAllocatedDicts(kvstore *kvs) {
    return kvs->allocated_dicts;
}

int kvstoreNumDicts(kvstore *kvs) {
    return kvs->num_dicts;
}

/* Move dict from one kvstore to another. */
void kvstoreMoveDict(kvstore *kvs, kvstore *dst, int didx) {
    assert(kvs->num_dicts > didx);
    assert(kvs->num_dicts == dst->num_dicts);
    assert(dst->dicts[didx] == NULL);

    hashtable *ht = kvs->dicts[didx];
    if (ht == NULL) return;

    /* Total buckets includes both top-level and chained (child) buckets,
     * matching what kvstoreDictTrackMemUsage accumulated. */
    size_t total_buckets = hashtableBuckets(ht)
                         + hashtableChainedBuckets(ht, 0)
                         + hashtableChainedBuckets(ht, 1);

    /* Adjust source kvstore */
    kvs->allocated_dicts -= 1;
    cumulativeKeyCountAdd(kvs, didx, -((long long)hashtableSize(ht)));
    kvs->bucket_count -= total_buckets;
    /* If rehashing, stop it. */
    if (hashtableIsRehashing(ht))
        kvstoreDictRehashingCompleted(ht);
    /* Clear dict from source kvstore and create a new one if needed */
    kvs->dicts[didx] = NULL;
    if (!(kvs->flags & (KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS)))
        createDictIfNeeded(kvs, didx);

    /* Move dict to destination kvstore */
    dst->dicts[didx] = ht;
    hashtableSetType(ht, &dst->htype);
    dst->allocated_dicts += 1;
    cumulativeKeyCountAdd(dst, didx, hashtableSize(ht));
    dst->bucket_count += total_buckets;
    if (hashtableIsRehashing(dst->dicts[didx]))
        kvstoreDictRehashingStarted(dst->dicts[didx]);
}

/* Returns kvstore iterator that can be used to iterate through sub-dictionaries.
 *
 * The caller should reset kvs_it with kvstoreIteratorReset. */
void kvstoreIteratorInit(kvstoreIterator *kvs_it, kvstore *kvs) {
    kvs_it->kvs = kvs;
    kvs_it->didx = -1;
    kvs_it->next_didx = kvstoreGetFirstNonEmptyDictIndex(kvs_it->kvs); /* Finds first non-empty dict index. */
    hashtableInitIterator(&kvs_it->di, NULL, HASHTABLE_ITER_SAFE);
}

/* Free the kvs_it returned by kvstoreIteratorInit. */
void kvstoreIteratorReset(kvstoreIterator *kvs_it) {
    hashtableCleanupIterator(&kvs_it->di);
    /* In the safe iterator context, we may delete entries. */
    if (kvs_it->didx != -1)
        freeDictIfNeeded(kvs_it->kvs, kvs_it->didx);
}

/* Returns next dictionary from the iterator, or NULL if iteration is complete.
 *
 * - Takes care to reset the iter of the previous dict before moved to the next dict.
 */
hashtable *kvstoreIteratorNextDict(kvstoreIterator *kvs_it) {
    if (kvs_it->next_didx == -1)
        return NULL;

    /* The dict may be deleted during the iteration process, so here need to check for NULL. */
    if (kvs_it->didx != -1 && kvstoreGetDict(kvs_it->kvs, kvs_it->didx)) {
        /* Before we move to the next dict, reset the iter of the previous dict. */
        hashtableCleanupIterator(&kvs_it->di);
        /* In the safe iterator context, we may delete entries. */
        freeDictIfNeeded(kvs_it->kvs, kvs_it->didx);
    }

    kvs_it->didx = kvs_it->next_didx;
    kvs_it->next_didx = kvstoreGetNextNonEmptyDictIndex(kvs_it->kvs, kvs_it->didx);
    return kvs_it->kvs->dicts[kvs_it->didx];
}

int kvstoreIteratorGetCurrentDictIndex(kvstoreIterator *kvs_it) {
    assert(kvs_it->didx >= 0 && kvs_it->didx < kvs_it->kvs->num_dicts);
    return kvs_it->didx;
}

/* Returns next entry. */
void *kvstoreIteratorNext(kvstoreIterator *kvs_it) {
    void *entry = NULL;
    /* hashtableNext returns false if the iterator is invalid or has no more entries */
    if (hashtableNext(&kvs_it->di, &entry)) {
        return entry;
    }
    /* No current dict or reached the end of the dictionary. */

    /* Before we move to the next dict, function kvstoreIteratorNextDict()
     * reset the iter of the previous dict & freeDictIfNeeded(). */
    hashtable *ht = kvstoreIteratorNextDict(kvs_it);

    if (!ht)
        return NULL;

    hashtableInitIterator(&kvs_it->di, ht, HASHTABLE_ITER_SAFE);
    hashtableNext(&kvs_it->di, &entry);
    return entry;
}

/* This method traverses through kvstore dictionaries and triggers a resize.
 * It first tries to shrink if needed, and if it isn't, it tries to expand. */
void kvstoreTryResizeDicts(kvstore *kvs, int limit) {
    if (limit > kvs->num_dicts)
        limit = kvs->num_dicts;

    for (int i = 0; i < limit; i++) {
        int didx = kvs->resize_cursor;
        hashtable *ht = kvstoreGetDict(kvs, didx);
        if (ht && !hashtableShrinkIfNeeded(ht)) {
            hashtableExpandIfNeeded(ht);
        }
        kvs->resize_cursor = (didx + 1) % kvs->num_dicts;
    }
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use threshold_us
 * of CPU time at every call of this function to perform some rehashing.
 *
 * The function returns the amount of microsecs spent if some rehashing was
 * performed, otherwise 0 is returned. */
uint64_t kvstoreIncrementallyRehash(kvstore *kvs, uint64_t threshold_us) {
    if (listLength(kvs->rehashing) == 0)
        return 0;

    /* Our goal is to rehash as many dictionaries as we can before reaching threshold_us,
     * after each dictionary completes rehashing, it removes itself from the list. */
    listNode *node;
    monotime timer;
    uint64_t elapsed_us = 0;
    elapsedStart(&timer);
    while ((node = listFirst(kvs->rehashing))) {
        hashtableRehashMicroseconds(listNodeValue(node), threshold_us - elapsed_us);

        elapsed_us = elapsedUs(timer);
        if (elapsed_us >= threshold_us) {
            break;  /* Reached the time limit. */
        }
    }
    return elapsed_us;
}

size_t kvstoreOverheadHashtableLut(kvstore *kvs) {
    return kvs->bucket_count * HASHTABLE_BUCKET_SIZE;
}

size_t kvstoreOverheadHashtableRehashing(kvstore *kvs) {
    return kvs->overhead_hashtable_rehashing;
}

unsigned long kvstoreDictRehashingCount(kvstore *kvs) {
    return listLength(kvs->rehashing);
}

unsigned long kvstoreDictSize(kvstore *kvs, int didx)
{
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableSize(ht);
}

void kvstoreInitDictIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx)
{
    kvs_di->kvs = kvs;
    kvs_di->didx = didx;
    hashtableInitIterator(&kvs_di->di, kvstoreGetDict(kvs, didx), 0);
}

void kvstoreInitDictSafeIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx)
{
    kvs_di->kvs = kvs;
    kvs_di->didx = didx;
    hashtableInitIterator(&kvs_di->di, kvstoreGetDict(kvs, didx), HASHTABLE_ITER_SAFE);
}

/* Free the kvs_di returned by kvstoreGetDictIterator and kvstoreGetDictSafeIterator. */
void kvstoreResetDictIterator(kvstoreDictIterator *kvs_di)
{
    /* The dict may be deleted during the iteration process, so here need to check for NULL. */
    if (kvstoreGetDict(kvs_di->kvs, kvs_di->didx)) {
        hashtableCleanupIterator(&kvs_di->di);
        /* In the safe iterator context, we may delete entries. */
        freeDictIfNeeded(kvs_di->kvs, kvs_di->didx);
    }
}

/* Get the next element of the dict through kvstoreDictIterator and hashtableNext. */
void *kvstoreDictIteratorNext(kvstoreDictIterator *kvs_di)
{
    /* The dict may be deleted during the iteration process, so here need to check for NULL. */
    hashtable *ht = kvstoreGetDict(kvs_di->kvs, kvs_di->didx);
    if (!ht) return NULL;

    void *entry = NULL;
    hashtableNext(&kvs_di->di, &entry);
    return entry;
}

int kvstoreDictGetRandomKey(kvstore *kvs, int didx, void **found)
{
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableRandomEntry(ht, found);
}

int kvstoreDictGetFairRandomKey(kvstore *kvs, int didx, void **found)
{
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableFairRandomEntry(ht, found);
}

unsigned int kvstoreDictGetSomeKeys(kvstore *kvs, int didx, void **dst, unsigned int count)
{
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableSampleEntries(ht, dst, count);
}

int kvstoreDictExpand(kvstore *kvs, int didx, unsigned long size)
{
    hashtable *ht = createDictIfNeeded(kvs, didx);
    if (!ht)
        return 0;
    return hashtableExpand(ht, size);
}

unsigned long kvstoreDictScanDefrag(kvstore *kvs, int didx, unsigned long v, hashtableScanFunction fn, void *(*defragfn)(void *), void *privdata, int flags)
{
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableScanDefrag(ht, v, fn, privdata, defragfn, flags);
}

/* Unlike kvstoreDictScanDefrag(), this method doesn't defrag the data(keys and values)
 * within dict, it only reallocates the memory used by the dict structure itself using 
 * the provided allocation function. This feature was added for the active defrag feature.
 *
 * With 16k dictionaries for cluster mode with 1 shard, this operation may require substantial time
 * to execute.  A "cursor" is used to perform the operation iteratively.  When first called, a
 * cursor value of 0 should be provided.  The return value is an updated cursor which should be
 * provided on the next iteration.  The operation is complete when 0 is returned.
 *
 * The 'defragfn' callback is called with a reference to the dict that callback can reallocate. */
unsigned long kvstoreDictLUTDefrag(kvstore *kvs, unsigned long cursor, kvstoreDictLUTDefragFunction *defragfn) {
    for (int didx = cursor; didx < kvs->num_dicts; didx++) {
        hashtable **d = kvstoreGetDictRef(kvs, didx), *newd;
        if (!*d)
            continue;
        if ((newd = defragfn(*d))) {
            *d = newd;

            /* After defragmenting the dict, update its corresponding
             * rehashing node in the kvstore's rehashing list. */
            kvstoreDictMetaBase *metadata = hashtableMetadata(*d);
            if (metadata->rehashing_node)
                metadata->rehashing_node->value = *d;
        }
        return (didx + 1);
    }
    return 0;
}

int kvstoreDictFind(kvstore *kvs, int didx, void *key, void **found) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableFind(ht, key, found);
}

/* Returns a pointer to the entry in the hashtable for in-place modification.
 * Returns NULL if the key is not found. */
void **kvstoreDictFindRef(kvstore *kvs, int didx, const void *key) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return NULL;
    return hashtableFindRef(ht, key);
}

int kvstoreDictAdd(kvstore *kvs, int didx, void *entry) {
    hashtable *ht = createDictIfNeeded(kvs, didx);
    if (hashtableAdd(ht, entry)) {
        cumulativeKeyCountAdd(kvs, didx, 1);
        return 1;
    }
    return 0;
}

/* Find and remove an entry, returning it to the caller without freeing.
 * Returns 1 if found (entry stored in *popped), 0 if not found. */
int kvstoreDictPop(kvstore *kvs, int didx, const void *key, void **popped) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    int ret = hashtablePop(ht, key, popped);
    if (ret) {
        cumulativeKeyCountAdd(kvs, didx, -1);
        freeDictIfNeeded(kvs, didx);
    }
    return ret;
}

/* Find position for insert. Returns 1 if position found (key doesn't exist),
 * 0 if key already exists (existing entry returned via *existing). */
int kvstoreDictFindPositionForInsert(kvstore *kvs, int didx, void *key, hashtablePosition *position, void **existing) {
    hashtable *ht = createDictIfNeeded(kvs, didx);
    return hashtableFindPositionForInsert(ht, key, position, existing);
}

void kvstoreDictInsertAtPosition(kvstore *kvs, int didx, void *entry, hashtablePosition *position) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    assert(ht != NULL);
    hashtableInsertAtPosition(ht, entry, position);
    cumulativeKeyCountAdd(kvs, didx, 1);
}

void **kvstoreDictTwoPhasePopFindRef(kvstore *kvs, int didx, const void *key, hashtablePosition *position) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return NULL;
    return hashtableTwoPhasePopFindRef(ht, key, position);
}

void kvstoreDictTwoPhasePopDelete(kvstore *kvs, int didx, hashtablePosition *position) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    hashtableTwoPhasePopDelete(ht, position);
    cumulativeKeyCountAdd(kvs, didx, -1);
    freeDictIfNeeded(kvs, didx);
}

int kvstoreDictDelete(kvstore *kvs, int didx, const void *key) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    int ret = hashtableDelete(ht, key);
    if (ret) {
        cumulativeKeyCountAdd(kvs, didx, -1);
        freeDictIfNeeded(kvs, didx);
    }
    return ret;
}

unsigned long kvstoreDictBuckets(kvstore *kvs, int didx) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht)
        return 0;
    return hashtableBuckets(ht);
}

void *kvstoreGetDictMeta(kvstore *kvs, int didx, int createIfNeeded) {
    hashtable *ht = kvstoreGetDict(kvs, didx);
    if (!ht) {
        if (!createIfNeeded) return NULL;
        ht = createDictIfNeeded(kvs, didx);
    }
    return hashtableMetadata(ht);
}

void *kvstoreGetMetadata(kvstore *kvs) {
    if (!kvs->type->kvstoreMetadataBytes)
        return NULL;
    return &kvs->metadata;
}

#ifdef REDIS_TEST
#include <stdio.h>
#include "testhelp.h"

#define TEST(name) printf("test — %s\n", name);

uint64_t hashTestCallback(const void *key) {
    return hashtableGenHashFunction(key, strlen(key));
}

int compareTestCallback(const void *key1, const void *key2) {
    return strcmp(key1, key2);
}

void freeTestCallback(hashtable *ht, void *entry) {
    UNUSED(ht);
    zfree(entry);
}

void *defragAllocTest(void *ptr) {
    size_t size = zmalloc_usable_size(ptr);
    void *newptr = zmalloc(size);
    memcpy(newptr, ptr, size);
    zfree(ptr);
    return newptr;
}

hashtable *defragLUTTestCallback(hashtable *ht) {
    /* For test purposes, we just return the hashtable as-is.
     * Real defrag would reallocate the internal structures. */
    return hashtableDefragTables(ht, defragAllocTest);
}

hashtableType KvstoreHashtableTestType = {
    .hashFunction = hashTestCallback,
    .keyCompare = compareTestCallback,
    .entryDestructor = freeTestCallback,
};

kvstoreType KvstoreTestType = {
    NULL, /* kvstore metadata size */
    NULL, /* dict metadata size */
    NULL, /* can free dict */
    NULL, /* on kvstore empty */
    NULL, /* on dict empty */
};

char *stringFromInt(int value) {
    char buf[32];
    int len;
    char *s;

    len = snprintf(buf, sizeof(buf), "%d",value);
    s = zmalloc(len+1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

/* ./redis-server test kvstore */
int kvstoreTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    char *key;
    kvstoreIterator kvs_it;
    kvstoreDictIterator kvs_di;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreTestType, &KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *kvs2 = kvstoreCreate(&KvstoreTestType, &KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS);

    TEST("Add 16 keys") {
        for (i = 0; i < 16; i++) {
            int ret = kvstoreDictAdd(kvs1, didx, stringFromInt(i));
            assert(ret == 1);
            ret = kvstoreDictAdd(kvs2, didx, stringFromInt(i));
            assert(ret == 1);
        }
        assert(kvstoreDictSize(kvs1, didx) == 16);
        assert(kvstoreSize(kvs1) == 16);
        assert(kvstoreDictSize(kvs2, didx) == 16);
        assert(kvstoreSize(kvs2) == 16);
    }

    TEST("kvstoreIterator creating and releasing without kvstoreIteratorNextDict()") {
        kvstore *kvs = kvstoreCreate(&KvstoreTestType, &KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS);
        kvstoreIterator kvs_iter;
        kvstoreIteratorInit(&kvs_iter, kvs);
        kvstoreIteratorReset(&kvs_iter);
        kvstoreRelease(kvs);
    }

    TEST("kvstoreIterator case 1: removing all keys does not delete the empty dict") {
        kvstoreIteratorInit(&kvs_it, kvs1);
        while((key = kvstoreIteratorNext(&kvs_it)) != NULL) {
            curr_slot = kvstoreIteratorGetCurrentDictIndex(&kvs_it);
            assert(kvstoreDictDelete(kvs1, curr_slot, key) == 1);
        }
        kvstoreIteratorReset(&kvs_it);

        hashtable *ht = kvstoreGetDict(kvs1, didx);
        assert(ht != NULL);
        assert(kvstoreDictSize(kvs1, didx) == 0);
        assert(kvstoreSize(kvs1) == 0);
    }

    TEST("kvstoreIterator case 2: removing all keys will delete the empty dict") {
        kvstoreIteratorInit(&kvs_it, kvs2);
        while((key = kvstoreIteratorNext(&kvs_it)) != NULL) {
            curr_slot = kvstoreIteratorGetCurrentDictIndex(&kvs_it);
            assert(kvstoreDictDelete(kvs2, curr_slot, key) == 1);
        }
        kvstoreIteratorReset(&kvs_it);

        /* Make sure the dict was removed from the rehashing list. */
        while (kvstoreIncrementallyRehash(kvs2, 1000)) {}

        hashtable *ht = kvstoreGetDict(kvs2, didx);
        assert(ht == NULL);
        assert(kvstoreDictSize(kvs2, didx) == 0);
        assert(kvstoreSize(kvs2) == 0);
    }

    TEST("Add 16 keys again") {
        for (i = 0; i < 16; i++) {
            int ret = kvstoreDictAdd(kvs1, didx, stringFromInt(i));
            assert(ret == 1);
            ret = kvstoreDictAdd(kvs2, didx, stringFromInt(i));
            assert(ret == 1);
        }
        assert(kvstoreDictSize(kvs1, didx) == 16);
        assert(kvstoreSize(kvs1) == 16);
        assert(kvstoreDictSize(kvs2, didx) == 16);
        assert(kvstoreSize(kvs2) == 16);
    }

    TEST("kvstoreDictIterator case 1: removing all keys does not delete the empty dict") {
        kvstoreInitDictSafeIterator(&kvs_di, kvs1, didx);
        while((key = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
            assert(kvstoreDictDelete(kvs1, didx, key) == 1);
        }
        kvstoreResetDictIterator(&kvs_di);

        hashtable *ht = kvstoreGetDict(kvs1, didx);
        assert(ht != NULL);
        assert(kvstoreDictSize(kvs1, didx) == 0);
        assert(kvstoreSize(kvs1) == 0);
    }

    TEST("kvstoreDictIterator case 2: removing all keys will delete the empty dict") {
        kvstoreInitDictSafeIterator(&kvs_di, kvs2, didx);
        while((key = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
            assert(kvstoreDictDelete(kvs2, didx, key) == 1);
        }
        kvstoreResetDictIterator(&kvs_di);

        hashtable *ht = kvstoreGetDict(kvs2, didx);
        assert(ht == NULL);
        assert(kvstoreDictSize(kvs2, didx) == 0);
        assert(kvstoreSize(kvs2) == 0);
    }

    TEST("Verify that a rehashing dict's node in the rehashing list is correctly updated after defragmentation") {
        unsigned long cursor = 0;
        kvstore *kvs = kvstoreCreate(&KvstoreTestType, &KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        for (i = 0; i < 256; i++) {
            int ret = kvstoreDictAdd(kvs, 0, stringFromInt(i));
            assert(ret == 1);
            if (listLength(kvs->rehashing)) break;
        }
        assert(listLength(kvs->rehashing));
        while ((cursor = kvstoreDictLUTDefrag(kvs, cursor, defragLUTTestCallback)) != 0) {}
        while (kvstoreIncrementallyRehash(kvs, 1000)) {}
        kvstoreRelease(kvs);
    }

    TEST("Verify non-empty dict count is correctly updated") {
        kvstore *kvs = kvstoreCreate(&KvstoreTestType, &KvstoreHashtableTestType, 2,
                            KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        for (int idx = 0; idx < 4; idx++) {
            for (i = 0; i < 16; i++) {
                int ret = kvstoreDictAdd(kvs, idx, stringFromInt(i));
                assert(ret == 1);
                /* When the first element is inserted, the number of non-empty dictionaries is increased by 1. */
                if (i == 0) assert(kvstoreNumNonEmptyDicts(kvs) == idx + 1);
            }
        }

        /* Step by step, clear all dictionaries and ensure non-empty dict count is updated */
        for (int idx = 0; idx < 4; idx++) {
            kvstoreInitDictSafeIterator(&kvs_di, kvs, idx);
            while((key = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
                assert(kvstoreDictDelete(kvs, idx, key) == 1);
                /* When the dictionary is emptied, the number of non-empty dictionaries is reduced by 1. */
                if (kvstoreDictSize(kvs, idx) == 0) assert(kvstoreNumNonEmptyDicts(kvs) == 3 - idx);
            }
            kvstoreResetDictIterator(&kvs_di);
        }
        kvstoreRelease(kvs);
    }

    kvstoreRelease(kvs1);
    kvstoreRelease(kvs2);
    return 0;
}
#endif
