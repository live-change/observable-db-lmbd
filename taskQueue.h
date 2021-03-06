//
// Created by m8 on 4/29/21.
//

#ifndef OBSERVABLE_DB_LMDB_TASKQUEUE_H
#define OBSERVABLE_DB_LMDB_TASKQUEUE_H

#include <vector>
#include <thread>
#include <functional>
#include <concurrentqueue/blockingconcurrentqueue.h>

extern std::vector<std::thread> lmdbThreadPool;
extern moodycamel::BlockingConcurrentQueue<std::function<void()>> taskQueue;

extern std::thread writeThread;
extern moodycamel::BlockingConcurrentQueue<std::function<void()>> writeQueue;

void workerThreadLoop();

void writeThreadLoop();


#endif //OBSERVABLE_DB_LMDB_TASKQUEUE_H
