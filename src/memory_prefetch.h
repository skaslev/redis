/*
 * Copyright (c) 2025-Present, Redis Ltd.
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

#ifndef MEMORY_PREFETCH_H
#define MEMORY_PREFETCH_H

#include <stddef.h>

struct client;
struct hashtable;

/* Cross-command batch prefetching */
void prefetchCommandsBatchInit(void);
int determinePrefetchCount(int len);
int addCommandToBatch(struct client *c);
void resetCommandsBatch(void);
void prefetchCommands(void);

/* Intra-command prefetch: prefetch hashtable lookup data for an array of keys.
 * Reuses the same incremental-find state machine as the cross-command path.
 *
 * nkeys must be <= HASHTABLE_PREFETCH_MAX_SIZE (the function asserts this).
 * Callers should batch larger inputs into chunks of this size or smaller. */
#define HASHTABLE_PREFETCH_MAX_SIZE 64
void hashtablePrefetchKeys(struct hashtable **hts, void **keys, size_t nkeys);

#endif /* MEMORY_PREFETCH_H */
