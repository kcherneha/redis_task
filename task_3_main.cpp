#include "task_3.h"
#include <iostream>

using namespace task_3;

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <consumer_count> <redis_host> <redis_port>" << std::endl;
    return 1;
  }
  int consumer_count = std::stoi(argv[1]);
  std::string redis_host = argv[2];
  int redis_port = std::stoi(argv[3]);
  std::string channel = "messages:published";
  signal(SIGINT, signal_handler);
  std::thread monitor_thread(monitor_throughput);
  std::vector<std::thread> consumers;
  for (int i = 0; i < consumer_count; ++i) {
    consumers.emplace_back(consumer_thread, "consumer_" + std::to_string(i + 1),
                           channel, redis_host, redis_port);
  }
  RedisConnection ctx(redis_host, redis_port);

  std::vector<std::thread> workers;
  int thread_pool_size = consumer_count;
  for (int i = 0; i < thread_pool_size; ++i) {
    workers.emplace_back(process_message_batch, std::ref(ctx));
  }
  for (auto &consumer : consumers)
    consumer.join();
  for (auto &worker : workers)
    worker.join();
  keep_running = false;
  monitor_thread.join();
  return 0;
}
