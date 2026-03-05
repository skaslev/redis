/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * KVSTORE
 * -------
 * Index-based KV store implementation. This file implements a KV store comprised
 * of an array of hashtables (see hashtable.c) The purpose of this KV store is to have easy
 * access to all keys that belong in the same dict (i.e. are in the same dict-index)
 *
 * For example, when Redis is running in cluster mode, we use kvstore to save
 * all keys that map to the same hash-slot in a separate dict within the kvstore
 * struct.
 * This enables us to easily access all keys that map to a specific hash-slot.
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#ifndef DICTARRAY_H_
#define DICTARRAY_H_

#include "hashtable.h"
#include "adlist.h"

typedef struct _kvstore kvstore;

/* Structure for kvstore iterator that allows iterating across multiple dicts. */
typedef struct _kvstoreIterator {
    kvstore *kvs;
    long long didx;
    long long next_didx;
    hashtableIterator di;
} kvstoreIterator;

/* Structure for kvstore dict iterator that allows iterating the corresponding dict. */
typedef struct _kvstoreDictIterator {
    kvstore *kvs;
    long long didx;
    hashtableIterator di;
} kvstoreDictIterator;

typedef int (kvstoreScanShouldSkipDict)(hashtable *d, int didx);
typedef int (kvstoreExpandShouldSkipDictIndex)(int didx);
typedef int (kvstoreRandomShouldSkipDictIndex)(int didx);

/* kvstoreType: Callback structure for kvstore-specific operations.
 * Similar to hashtableType, this allows kvstore to be a generic data structure
 * without hardcoding dependencies on specific subsystems. */
typedef struct kvstoreType {
    /* Allow kvstore to carry extra caller-defined metadata. The
     * extra memory is initialized to 0 when a kvstore is allocated. */
    size_t (*kvstoreMetadataBytes)(kvstore *kvs);

    /* Allow a per slot dicts to carry extra caller-defined metadata. The
     * extra memory is initialized to 0 when each dict is allocated. */
    size_t (*dictMetadataBytes)(void);

    /* Check if a dict at given index can be freed. Used by kvstore when
     * KVSTORE_FREE_EMPTY_DICTS is set. Return 1 if can free, 0 otherwise.
     * Parameters: kvstore pointer, dict index */
    int (*canFreeDict)(kvstore *kvs, int didx);

    /* Called when kvstore becomes empty. */
    void (*onKvstoreEmpty)(kvstore *kvs);

    /* Called when per slot dict becomes empty. Parameters: kvstore pointer,
     * dict index. */
    void (*onDictEmpty)(kvstore *kvs, int didx);
} kvstoreType;

/* Basic metadata allocated per dict.
 * If additional metadata needed, embed this structure as the first member
 * in a new, larger structure. */
typedef struct {
    listNode *rehashing_node;   /* list node in rehashing list */
} kvstoreDictMetaBase;

#define KVSTORE_ALLOCATE_DICTS_ON_DEMAND (1<<0)
#define KVSTORE_FREE_EMPTY_DICTS (1<<1)
kvstore *kvstoreCreate(kvstoreType *type, hashtableType *dtype, int num_dicts_bits, int flags);
void kvstoreEmpty(kvstore *kvs, void(callback)(hashtable*));
void kvstoreRelease(kvstore *kvs);
unsigned long long kvstoreSize(kvstore *kvs);
unsigned long kvstoreBuckets(kvstore *kvs);
size_t kvstoreMemUsage(kvstore *kvs);
unsigned long long kvstoreScan(kvstore *kvs, unsigned long long cursor,
                               int onlydidx, hashtableScanFunction scan_cb,
                               kvstoreScanShouldSkipDict *skip_cb,
                               void *privdata);
int kvstoreExpand(kvstore *kvs, uint64_t newsize, int try_expand, kvstoreExpandShouldSkipDictIndex *skip_cb);
int kvstoreGetFairRandomDictIndex(kvstore *kvs, kvstoreExpandShouldSkipDictIndex *skip_cb,
                                  int fair_attempts, int slow_fallback);
void kvstoreGetStats(kvstore *kvs, char *buf, size_t bufsize, int full);

int kvstoreFindDictIndexByKeyIndex(kvstore *kvs, unsigned long target);
int kvstoreGetFirstNonEmptyDictIndex(kvstore *kvs);
int kvstoreGetNextNonEmptyDictIndex(kvstore *kvs, int didx);
int kvstoreNumNonEmptyDicts(kvstore *kvs);
int kvstoreNumAllocatedDicts(kvstore *kvs);
int kvstoreNumDicts(kvstore *kvs);
void kvstoreMoveDict(kvstore *kvs, kvstore *dst, int didx);

/* kvstore iterator specific functions */
void kvstoreIteratorInit(kvstoreIterator *kvs_it, kvstore *kvs);
void kvstoreIteratorReset(kvstoreIterator *kvs_it);
hashtable *kvstoreIteratorNextDict(kvstoreIterator *kvs_it);
int kvstoreIteratorGetCurrentDictIndex(kvstoreIterator *kvs_it);
void *kvstoreIteratorNext(kvstoreIterator *kvs_it);

/* Rehashing */
void kvstoreTryResizeDicts(kvstore *kvs, int limit);
uint64_t kvstoreIncrementallyRehash(kvstore *kvs, uint64_t threshold_us);
size_t kvstoreOverheadHashtableLut(kvstore *kvs);
size_t kvstoreOverheadHashtableRehashing(kvstore *kvs);
unsigned long kvstoreDictRehashingCount(kvstore *kvs);

/* Specific dict access by dict-index */
unsigned long kvstoreDictSize(kvstore *kvs, int didx);
void kvstoreInitDictIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx);
void kvstoreInitDictSafeIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx);
void kvstoreResetDictIterator(kvstoreDictIterator *kvs_di);
void *kvstoreDictIteratorNext(kvstoreDictIterator *kvs_di);
int kvstoreDictGetRandomKey(kvstore *kvs, int didx, void **found);
int kvstoreDictGetFairRandomKey(kvstore *kvs, int didx, void **found);
unsigned int kvstoreDictGetSomeKeys(kvstore *kvs, int didx, void **dst, unsigned int count);
int kvstoreDictExpand(kvstore *kvs, int didx, unsigned long size);
unsigned long kvstoreDictScanDefrag(kvstore *kvs, int didx, unsigned long v, hashtableScanFunction fn, void *(*defragfn)(void *), void *privdata, int flags);
typedef hashtable *(kvstoreDictLUTDefragFunction)(hashtable *d);
unsigned long kvstoreDictLUTDefrag(kvstore *kvs, unsigned long cursor, kvstoreDictLUTDefragFunction *defragfn);
int kvstoreDictFind(kvstore *kvs, int didx, void *key, void **found);
void **kvstoreDictFindRef(kvstore *kvs, int didx, const void *key);
int kvstoreDictAdd(kvstore *kvs, int didx, void *entry);
int kvstoreDictPop(kvstore *kvs, int didx, const void *key, void **popped);
void **kvstoreDictTwoPhasePopFindRef(kvstore *kvs, int didx, const void *key, hashtablePosition *position);
void kvstoreDictTwoPhasePopDelete(kvstore *kvs, int didx, hashtablePosition *position);
int kvstoreDictFindPositionForInsert(kvstore *kvs, int didx, void *key, hashtablePosition *position, void **existing);
void kvstoreDictInsertAtPosition(kvstore *kvs, int didx, void *entry, hashtablePosition *position);
int kvstoreDictDelete(kvstore *kvs, int didx, const void *key);
unsigned long kvstoreDictBuckets(kvstore *kvs, int didx);
hashtable *kvstoreGetDict(kvstore *kvs, int didx);
void kvstoreFreeDictIfNeeded(kvstore *kvs, int didx);
void *kvstoreGetDictMeta(kvstore *kvs, int didx, int createIfNeeded);
void *kvstoreGetMetadata(kvstore *kvs);

#ifdef REDIS_TEST
int kvstoreTest(int argc, char *argv[], int flags);
#endif

#endif /* DICTARRAY_H_ */
