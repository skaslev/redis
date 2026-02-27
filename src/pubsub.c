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
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#include "server.h"
#include "cluster.h"
#include "cluster_slot_stats.h"

/* Structure to hold the pubsub related metadata. Currently used
 * for pubsub and pubsubshard feature. */
typedef struct pubsubtype {
    int shard;
    hashtable *(*clientPubSubChannels)(client*);
    int (*subscriptionCount)(client*);
    kvstore **serverPubSubChannels;
    robj **subscribeMsg;
    robj **unsubscribeMsg;
    robj **messageBulk;
}pubsubtype;

static pubsubEntry *createPubsubEntry(robj *channel) {
    pubsubEntry *entry = zmalloc(sizeof(*entry));
    entry->channel = channel;
    incrRefCount(channel);
    entry->clients = hashtableCreate(&clientHashtableType);
    return entry;
}

static void freePubsubEntry(pubsubEntry *entry) {
    if (entry->channel) decrRefCount(entry->channel);
    if (entry->clients) hashtableRelease(entry->clients);
    zfree(entry);
}

/* Hashtable callbacks for pubsub entries */
static const void *pubsubEntryGetKey(const void *entry) {
    return ((const pubsubEntry *)entry)->channel;
}

static void pubsubEntryDestructor(hashtable *ht, void *entry) {
    UNUSED(ht);
    freePubsubEntry(entry);
}

/* Hash function for robj (channel name) */
static uint64_t htObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

/* Compare two robj keys. Returns 0 if equal. */
static int htObjCompare(const void *key1, const void *key2) {
    const robj *o1 = key1, *o2 = key2;
    size_t l1 = sdslen((sds)o1->ptr);
    size_t l2 = sdslen((sds)o2->ptr);
    if (l1 != l2) return 1;
    return memcmp(o1->ptr, o2->ptr, l1);
}

/* Hashtable type for pubsub channels kvstore - entries are pubsubEntry* */
hashtableType pubsubHashtableType = {
    .entryGetKey = pubsubEntryGetKey,
    .hashFunction = htObjHash,
    .keyCompare = htObjCompare,
    .entryDestructor = pubsubEntryDestructor,
    .resizeAllowed = NULL,
};

/* Destructor for client pubsub channel entries (robj) */
static void htObjDestructor(hashtable *ht, void *entry) {
    UNUSED(ht);
    decrRefCount((robj *)entry);
}

/* Hashtable type for client pubsub channels - entries are robj* */
hashtableType clientPubSubChannelsType = {
    .hashFunction = htObjHash,
    .keyCompare = htObjCompare,
    .entryDestructor = htObjDestructor,
};

/*
 * Get client's global Pub/Sub channels subscription count.
 */
int clientSubscriptionsCount(client *c);

/*
 * Get client's shard level Pub/Sub channels subscription count.
 */
int clientShardSubscriptionsCount(client *c);

/*
 * Get client's global Pub/Sub channels hashtable.
 */
hashtable *getClientPubSubChannels(client *c);

/*
 * Get client's shard level Pub/Sub channels hashtable.
 */
hashtable *getClientPubSubShardChannels(client *c);

/*
 * Get list of channels client is subscribed to.
 * If a pattern is provided, the subset of channels is returned
 * matching the pattern.
 */
void channelList(client *c, sds pat, kvstore *pubsub_channels);

/*
 * Pub/Sub type for global channels.
 */
pubsubtype pubSubType = {
    .shard = 0,
    .clientPubSubChannels = getClientPubSubChannels,
    .subscriptionCount = clientSubscriptionsCount,
    .serverPubSubChannels = &server.pubsub_channels,
    .subscribeMsg = &shared.subscribebulk,
    .unsubscribeMsg = &shared.unsubscribebulk,
    .messageBulk = &shared.messagebulk,
};

/*
 * Pub/Sub type for shard level channels bounded to a slot.
 */
pubsubtype pubSubShardType = {
    .shard = 1,
    .clientPubSubChannels = getClientPubSubShardChannels,
    .subscriptionCount = clientShardSubscriptionsCount,
    .serverPubSubChannels = &server.pubsubshard_channels,
    .subscribeMsg = &shared.ssubscribebulk,
    .unsubscribeMsg = &shared.sunsubscribebulk,
    .messageBulk = &shared.smessagebulk,
};

/*-----------------------------------------------------------------------------
 * Pubsub client replies API
 *----------------------------------------------------------------------------*/

/* Send a pubsub message of type "message" to the client.
 * Normally 'msg' is a Redis object containing the string to send as
 * message. However if the caller sets 'msg' as NULL, it will be able
 * to send a special message (for instance an Array type) by using the
 * addReply*() API family. */
void addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,message_bulk);
    addReplyBulk(c,channel);
    if (msg) addReplyBulk(c,msg);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send a pubsub message of type "pmessage" to the client. The difference
 * with the "message" type delivered by addReplyPubsubMessage() is that
 * this message format also includes the pattern that matched the message. */
void addReplyPubsubPatMessage(client *c, robj *pat, robj *channel, robj *msg) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[4]);
    else
        addReplyPushLen(c,4);
    addReply(c,shared.pmessagebulk);
    addReplyBulk(c,pat);
    addReplyBulk(c,channel);
    addReplyBulk(c,msg);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub subscription notification to the client. */
void addReplyPubsubSubscribed(client *c, robj *channel, pubsubtype type) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,*type.subscribeMsg);
    addReplyBulk(c,channel);
    addReplyLongLong(c,type.subscriptionCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub unsubscription notification to the client.
 * Channel can be NULL: this is useful when the client sends a mass
 * unsubscribe command but there are no channels to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubUnsubscribed(client *c, robj *channel, pubsubtype type) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c, *type.unsubscribeMsg);
    if (channel)
        addReplyBulk(c,channel);
    else
        addReplyNull(c);
    addReplyLongLong(c,type.subscriptionCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub pattern subscription notification to the client. */
void addReplyPubsubPatSubscribed(client *c, robj *pattern) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.psubscribebulk);
    addReplyBulk(c,pattern);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub pattern unsubscription notification to the client.
 * Pattern can be NULL: this is useful when the client sends a mass
 * punsubscribe command but there are no pattern to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubPatUnsubscribed(client *c, robj *pattern) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.punsubscribebulk);
    if (pattern)
        addReplyBulk(c,pattern);
    else
        addReplyNull(c);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/*-----------------------------------------------------------------------------
 * Pubsub low level API
 *----------------------------------------------------------------------------*/

/* Return the number of pubsub channels + patterns is handled. */
int serverPubsubSubscriptionCount(void) {
    return kvstoreSize(server.pubsub_channels) + dictSize(server.pubsub_patterns);
}

/* Return the number of pubsub shard level channels is handled. */
int serverPubsubShardSubscriptionCount(void) {
    return kvstoreSize(server.pubsubshard_channels);
}

/* Return the number of channels + patterns a client is subscribed to. */
int clientSubscriptionsCount(client *c) {
    return hashtableSize(c->pubsub_channels) + hashtableSize(c->pubsub_patterns);
}

/* Return the number of shard level channels a client is subscribed to. */
int clientShardSubscriptionsCount(client *c) {
    return hashtableSize(c->pubsubshard_channels);
}

hashtable *getClientPubSubChannels(client *c) {
    return c->pubsub_channels;
}

hashtable *getClientPubSubShardChannels(client *c) {
    return c->pubsubshard_channels;
}

/* Return the number of pubsub + pubsub shard level channels
 * a client is subscribed to. */
int clientTotalPubSubSubscriptionCount(client *c) {
    return clientSubscriptionsCount(c) + clientShardSubscriptionsCount(c);
}

void markClientAsPubSub(client *c) {
    if (!(c->flags & CLIENT_PUBSUB)) {
        c->flags |= CLIENT_PUBSUB;
        server.pubsub_clients++;
    }
}

void unmarkClientAsPubSub(client *c) {
    if (c->flags & CLIENT_PUBSUB) {
        c->flags &= ~CLIENT_PUBSUB;
        server.pubsub_clients--;
    }
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
int pubsubSubscribeChannel(client *c, robj *channel, pubsubtype type) {
    hashtable *clients = NULL;
    int retval = 0;
    unsigned int slot = 0;

    /* Add the channel to the client -> channels hash table */
    hashtablePosition htpos;
    if (hashtableFindPositionForInsert(type.clientPubSubChannels(c), channel, &htpos, NULL)) {
        /* Not yet subscribed to this channel */
        retval = 1;
        /* Add the client to the channel -> list of clients hash table */
        if (server.cluster_enabled && type.shard) {
            slot = getKeySlot(channel->ptr);
        }

        /* Look for existing entry or find position for insert */
        hashtablePosition pos;
        void *existing;
        if (!kvstoreDictFindPositionForInsert(*type.serverPubSubChannels, slot, channel, &pos, &existing)) {
            /* Channel already exists */
            pubsubEntry *entry = existing;
            clients = entry->clients;
            channel = entry->channel;
        } else {
            /* Create new pubsub entry and insert at position */
            pubsubEntry *entry = createPubsubEntry(channel);
            clients = entry->clients;
            kvstoreDictInsertAtPosition(*type.serverPubSubChannels, slot, entry, &pos);
        }

        serverAssert(hashtableAdd(clients, c));
        incrRefCount(channel);
        hashtableInsertAtPosition(type.clientPubSubChannels(c), channel, &htpos);
    }
    /* Notify the client */
    addReplyPubsubSubscribed(c,channel,type);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify, pubsubtype type) {
    hashtable *clients;
    int retval = 0;
    int slot = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    if (hashtableDelete(type.clientPubSubChannels(c), channel)) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        if (server.cluster_enabled && type.shard) {
            /* Compute the slot from the channel directly instead of using getKeySlot(),
             * because the unsubscribe may be triggered by a different client, and
             * getKeySlot() would return the cached slot of that client. */
            slot = keyHashSlot(channel->ptr, sdslen(channel->ptr));
        }
        void *entry;
        int found = kvstoreDictFind(*type.serverPubSubChannels, slot, channel, &entry);
        serverAssertWithInfo(c, NULL, found);
        pubsubEntry *pe = entry;
        clients = pe->clients;
        serverAssertWithInfo(c, NULL, hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) {
            /* Free the entry if this was the latest client, so that it will
             * be possible to abuse Redis PUBSUB creating millions of channels. */
            kvstoreDictDelete(*type.serverPubSubChannels, slot, channel);
        }
    }
    /* Notify the client */
    if (notify) {
        addReplyPubsubUnsubscribed(c,channel,type);
    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Unsubscribe all shard channels in a slot. */
void pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot) {
    if (!kvstoreDictSize(server.pubsubshard_channels, slot))
        return;

    void *entry;
    kvstoreDictIterator kvs_di;
    kvstoreInitDictSafeIterator(&kvs_di, server.pubsubshard_channels, slot);
    while ((entry = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
        pubsubEntry *pe = entry;
        robj *channel = pe->channel;
        hashtable *clients = pe->clients;
        /* For each client subscribed to the channel, unsubscribe it. */
        hashtableIterator iter;
        void *client_entry;

        hashtableInitIterator(&iter, clients, 0);
        while (hashtableNext(&iter, &client_entry)) {
            client *c = client_entry;
            int deleted = hashtableDelete(c->pubsubshard_channels, channel);
            serverAssertWithInfo(c, channel, deleted);
            addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
            /* If the client has no other pubsub subscription,
             * move out of pubsub mode. */
            if (clientTotalPubSubSubscriptionCount(c) == 0) {
                unmarkClientAsPubSub(c);
            }
        }
        hashtableCleanupIterator(&iter);
        kvstoreDictDelete(server.pubsubshard_channels, slot, channel);
    }
    kvstoreResetDictIterator(&kvs_di);
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. */
int pubsubSubscribePattern(client *c, robj *pattern) {
    dictEntry *de;
    hashtable *clients;
    int retval = 0;

    if (hashtableAdd(c->pubsub_patterns, pattern)) {
        retval = 1;
        incrRefCount(pattern);
        /* Add the client to the pattern -> list of clients hash table */
        de = dictFind(server.pubsub_patterns,pattern);
        if (de == NULL) {
            clients = hashtableCreate(&clientHashtableType);
            dictAdd(server.pubsub_patterns,pattern,clients);
            incrRefCount(pattern);
        } else {
            clients = dictGetVal(de);
        }
        serverAssert(hashtableAdd(clients, c));
    }
    /* Notify the client */
    addReplyPubsubPatSubscribed(c,pattern);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify) {
    dictEntry *de;
    hashtable *clients;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    if (hashtableDelete(c->pubsub_patterns, pattern)) {
        retval = 1;
        /* Remove the client from the pattern -> clients list hash table */
        de = dictFind(server.pubsub_patterns,pattern);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        serverAssertWithInfo(c, NULL, hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) {
            /* Free the hashtable and associated hash entry at all if this was
             * the latest client. */
            dictDelete(server.pubsub_patterns,pattern);
        }
    }
    /* Notify the client */
    if (notify) addReplyPubsubPatUnsubscribed(c,pattern);
    decrRefCount(pattern);
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
int pubsubUnsubscribeAllChannelsInternal(client *c, int notify, pubsubtype type) {
    int count = 0;
    if (hashtableSize(type.clientPubSubChannels(c)) > 0) {
        hashtableIterator iter;
        void *entry;

        hashtableInitIterator(&iter, type.clientPubSubChannels(c), HASHTABLE_ITER_SAFE);
        while (hashtableNext(&iter, &entry)) {
            robj *channel = entry;
            count += pubsubUnsubscribeChannel(c,channel,notify,type);
        }
        hashtableCleanupIterator(&iter);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReplyPubsubUnsubscribed(c,NULL,type);
    }
    return count;
}

/*
 * Unsubscribe a client from all global channels.
 */
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c,notify,pubSubType);
    return count;
}

/*
 * Unsubscribe a client from all shard subscribed channels.
 */
int pubsubUnsubscribeShardAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c, notify, pubSubShardType);
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
int pubsubUnsubscribeAllPatterns(client *c, int notify) {
    int count = 0;

    if (hashtableSize(c->pubsub_patterns) > 0) {
        hashtableIterator iter;
        void *entry;

        hashtableInitIterator(&iter, c->pubsub_patterns, HASHTABLE_ITER_SAFE);
        while (hashtableNext(&iter, &entry)) {
            robj *pattern = entry;
            count += pubsubUnsubscribePattern(c, pattern, notify);
        }
        hashtableCleanupIterator(&iter);
    }

    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) addReplyPubsubPatUnsubscribed(c,NULL);
    return count;
}

/*
 * Publish a message to all the subscribers.
 */
int pubsubPublishMessageInternal(robj *channel, robj *message, pubsubtype type) {
    int receivers = 0;
    dictEntry *de;
    dictIterator di;
    unsigned int slot = 0;

    /* Send to clients listening for that channel */
    if (server.cluster_enabled && type.shard) {
        slot = keyHashSlot(channel->ptr, sdslen(channel->ptr));
    }
    void *entry;
    if (kvstoreDictFind(*type.serverPubSubChannels, slot, channel, &entry)) {
        pubsubEntry *pe = entry;
        hashtable *clients = pe->clients;
        hashtableIterator iter;
        void *client_entry;

        hashtableInitIterator(&iter, clients, 0);
        while (hashtableNext(&iter, &client_entry)) {
            client *c = client_entry;
            addReplyPubsubMessage(c,channel,message,*type.messageBulk);
            if (clusterSlotStatsEnabled(CLUSTER_SLOT_STATS_NET))
                clusterSlotStatsAddNetworkBytesOutForShardedPubSubInternalPropagation(c, slot);
            updateClientMemUsageAndBucket(c);
            receivers++;
        }
        hashtableCleanupIterator(&iter);
    }

    if (type.shard) {
        /* Shard pubsub ignores patterns. */
        return receivers;
    }

    /* Send to clients listening to matching channels */
    if (dictSize(server.pubsub_patterns) > 0) {
        channel = getDecodedObject(channel);
        dictInitIterator(&di, server.pubsub_patterns);
        while((de = dictNext(&di)) != NULL) {
            robj *pattern = dictGetKey(de);
            hashtable *clients = dictGetVal(de);
            if (!stringmatchlen((char*)pattern->ptr,
                                sdslen(pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) continue;

            hashtableIterator iter;
            void *client_entry;

            hashtableInitIterator(&iter, clients, 0);
            while (hashtableNext(&iter, &client_entry)) {
                client *c = client_entry;
                addReplyPubsubPatMessage(c,pattern,channel,message);
                updateClientMemUsageAndBucket(c);
                receivers++;
            }
            hashtableCleanupIterator(&iter);
        }
        decrRefCount(channel);
        dictResetIterator(&di);
    }
    return receivers;
}

/* Publish a message to all the subscribers. */
int pubsubPublishMessage(robj *channel, robj *message, int sharded) {
    return pubsubPublishMessageInternal(channel, message, sharded? pubSubShardType : pubSubType);
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

/* SUBSCRIBE channel [channel ...] */
void subscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backward compatibility
         */
        addReplyError(c, "SUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j],pubSubType);
    markClientAsPubSub(c);
}

/* UNSUBSCRIBE [channel ...] */
void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1,pubSubType);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* PSUBSCRIBE pattern [pattern ...] */
void psubscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backward compatibility
         */
        addReplyError(c, "PSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    markClientAsPubSub(c);
}

/* PUNSUBSCRIBE [pattern [pattern ...]] */
void punsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* This function wraps pubsubPublishMessage and also propagates the message to cluster.
 * Used by the commands PUBLISH/SPUBLISH and their respective module APIs.*/
int pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded) {
    int receivers = pubsubPublishMessage(channel, message, sharded);
    if (server.cluster_enabled)
        clusterPropagatePublish(channel, message, sharded);
    return receivers;
}

/* PUBLISH <channel> <message> */
void publishCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelPublishCommand(c);
        return;
    }

    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],0);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* PUBSUB command for Pub/Sub introspection. */
void pubsubCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CHANNELS [<pattern>]",
"    Return the currently active channels matching a <pattern> (default: '*').",
"NUMPAT",
"    Return number of subscriptions to patterns.",
"NUMSUB [<channel> ...]",
"    Return the number of subscribers for the specified channels, excluding",
"    pattern subscriptions(default: no channels).",
"SHARDCHANNELS [<pattern>]",
"    Return the currently active shard level channels matching a <pattern> (default: '*').",
"SHARDNUMSUB [<shardchannel> ...]",
"    Return the number of subscribers for the specified shard level channel(s)",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"channels") &&
        (c->argc == 2 || c->argc == 3))
    {
        /* PUBSUB CHANNELS [<pattern>] */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        channelList(c, pat, server.pubsub_channels);
    } else if (!strcasecmp(c->argv[1]->ptr,"numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N] */
        int j;

        addReplyArrayLen(c,(c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            void *entry;
            hashtable *clients = NULL;
            if (kvstoreDictFind(server.pubsub_channels, 0, c->argv[j], &entry)) {
                clients = ((pubsubEntry *)entry)->clients;
            }

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c, clients ? hashtableSize(clients) : 0);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT */
        addReplyLongLong(c,dictSize(server.pubsub_patterns));
    } else if (!strcasecmp(c->argv[1]->ptr,"shardchannels") &&
        (c->argc == 2 || c->argc == 3)) 
    {
        /* PUBSUB SHARDCHANNELS */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        channelList(c,pat,server.pubsubshard_channels);
    } else if (!strcasecmp(c->argv[1]->ptr,"shardnumsub") && c->argc >= 2) {
        /* PUBSUB SHARDNUMSUB [ShardChannel_1 ... ShardChannel_N] */
        int j;
        addReplyArrayLen(c, (c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            unsigned int slot = calculateKeySlot(c->argv[j]->ptr);
            void *entry;
            hashtable *clients = NULL;
            if (kvstoreDictFind(server.pubsubshard_channels, slot, c->argv[j], &entry)) {
                clients = ((pubsubEntry *)entry)->clients;
            }

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c, clients ? hashtableSize(clients) : 0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

void channelList(client *c, sds pat, kvstore *pubsub_channels) {
    long mblen = 0;
    void *replylen;
    unsigned int slot_cnt = kvstoreNumDicts(pubsub_channels);

    replylen = addReplyDeferredLen(c);
    for (unsigned int i = 0; i < slot_cnt; i++) {
        if (!kvstoreDictSize(pubsub_channels, i))
            continue;
        void *entry;
        kvstoreDictIterator kvs_di;
        kvstoreInitDictIterator(&kvs_di, pubsub_channels, i);
        while((entry = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
            pubsubEntry *pe = entry;
            robj *cobj = pe->channel;
            sds channel = cobj->ptr;

            if (!pat || stringmatchlen(pat, sdslen(pat),
                                    channel, sdslen(channel),0))
            {
                addReplyBulk(c,cobj);
                mblen++;
            }
        }
        kvstoreResetDictIterator(&kvs_di);
    }
    setDeferredArrayLen(c,replylen,mblen);
}

/* SPUBLISH <shardchannel> <message> */
void spublishCommand(client *c) {
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],1);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* SSUBSCRIBE shardchannel [shardchannel ...] */
void ssubscribeCommand(client *c) {
    if (c->flags & CLIENT_DENY_BLOCKING) {
        /* A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe. */
        addReplyError(c, "SSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (int j = 1; j < c->argc; j++) {
        pubsubSubscribeChannel(c, c->argv[j], pubSubShardType);
    }
    markClientAsPubSub(c);
}

/* SUNSUBSCRIBE [shardchannel [shardchannel ...]] */
void sunsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeShardAllChannels(c, 1);
    } else {
        for (int j = 1; j < c->argc; j++) {
            pubsubUnsubscribeChannel(c, c->argv[j], 1, pubSubShardType);
        }
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

size_t pubsubMemOverhead(client *c) {
    /* PubSub patterns */
    size_t mem = hashtableMemUsage(c->pubsub_patterns);
    /* Global PubSub channels */
    mem += hashtableMemUsage(c->pubsub_channels);
    /* Sharded PubSub channels */
    mem += hashtableMemUsage(c->pubsubshard_channels);
    return mem;
}

int pubsubTotalSubscriptions(void) {
    return dictSize(server.pubsub_patterns) +
           kvstoreSize(server.pubsub_channels) +
           kvstoreSize(server.pubsubshard_channels);
}
