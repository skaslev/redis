#include "fifo_queue.h"
#include "zmalloc.h"
#include "config.h"

#define FIFO_NODE_ITEMS 15

typedef struct fifoNode {
    void *items[FIFO_NODE_ITEMS];
    struct fifoNode *next;
} fifoNode;

struct fifoQueue {
    fifoNode *head;
    fifoNode *tail;
    int headIdx;
    int tailIdx;
    size_t size;
};

fifoQueue *fifoQueueCreate(void) {
    fifoQueue *fq = zmalloc(sizeof(*fq));
    fq->head = NULL;
    fq->tail = NULL;
    fq->headIdx = 0;
    fq->tailIdx = 0;
    fq->size = 0;
    return fq;
}

void fifoQueueDestroy(fifoQueue *fq) {
    fifoNode *node = fq->head;
    while (node) {
        fifoNode *next = node->next;
        zfree(node);
        node = next;
    }
    zfree(fq);
}

size_t fifoQueueSize(fifoQueue *fq) {
    return fq->size;
}

void fifoQueueEnqueue(fifoQueue *fq, void *data) {
    if (fq->tail == NULL) {
        fq->head = fq->tail = zcalloc(sizeof(fifoNode));
        fq->headIdx = 0;
        fq->tailIdx = 0;
    } else if (fq->tailIdx == FIFO_NODE_ITEMS) {
        fifoNode *newNode = zcalloc(sizeof(fifoNode));
        fq->tail->next = newNode;
        fq->tail = newNode;
        fq->tailIdx = 0;
    }

    fq->tail->items[fq->tailIdx++] = data;
    fq->size++;
}

void *fifoQueueDequeue(fifoQueue *fq) {
    if (fq->size == 0) return NULL;

    void *data = fq->head->items[fq->headIdx++];
    fq->size--;

    if (fq->head == fq->tail) {
        if (fq->size == 0) {
            // Queue is empty, reset indices to reuse the node
            fq->headIdx = 0;
            fq->tailIdx = 0;
        }
    } else if (fq->headIdx == FIFO_NODE_ITEMS) {
        // Head node exhausted and it's not the last node
        fifoNode *oldHead = fq->head;
        fq->head = oldHead->next;
        zfree(oldHead);
        fq->headIdx = 0;
        if (fq->head != NULL) {
            for (int i = 0; i < FIFO_NODE_ITEMS; i++) {
                redis_prefetch_read(fq->head->items[i]);
            }
        }
    }

    return data;
}
