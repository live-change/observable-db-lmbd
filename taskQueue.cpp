//
// Created by m8 on 4/29/21.
//

#include "taskQueue.h"

std::vector<std::thread> lmdbThreadPool;
std::thread writeThread;
moodycamel::BlockingConcurrentQueue<std::function<void()>> taskQueue;
moodycamel::BlockingConcurrentQueue<std::function<void()>> writeQueue;

void workerThreadLoop() {
  while(true) {
    std::function<void()> function;
    taskQueue.wait_dequeue(function);
    function();
  }
}

void writeThreadLoop() {
  while(true) {
    std::function<void()> function;
    writeQueue.wait_dequeue(function);
    function();
  }
}
