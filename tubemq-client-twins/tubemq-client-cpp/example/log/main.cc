
#include <chrono>
#include <exception>
#include <iostream>
#include <string>
#include <thread>

#include "logger.h"

using namespace std;
using namespace tubemq;

void log() {
  int i = 0;
  while (1) {
    LOG_ERROR("threadid:%ld, i:%d", std::this_thread::get_id(), i++);
  }
}

int main() {
  std::thread t1(log);
  std::thread t2(log);
  std::thread t3(log);
  std::thread t4(log);
  t1.join();
  return 0;
}

