//
// Created by m8 on 4/29/21.
//

#include "taskQueue.h"

std::vector<std::thread> lmdbThreadPool;
moodycamel::BlockingConcurrentQueue<std::function<void()>> taskQueue;

void workerThread() {
  while(true) {
    std::function<void()> function;
    taskQueue.wait_dequeue(function);
    function();
  }
}
