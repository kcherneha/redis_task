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

  // Start throughput monitor
  std::thread monitor_thread(monitor_throughput);

  // Start consumers
  std::vector<std::thread> consumers;
  try {
    for (int i = 0; i < consumer_count; ++i) {
      consumers.emplace_back(consumer_thread,
                             "consumer_" + std::to_string(i + 1), channel,
                             redis_host, redis_port);
    }
  } catch (const std::exception &e) {
    std::cerr << "Error starting consumers: " << e.what() << std::endl;
    keep_running = false;
    monitor_thread.join();
    return 1;
  }

  // Start workers with isolated Redis connections
  std::vector<std::thread> workers;
  try {
    for (int i = 0; i < consumer_count; ++i) {
      workers.emplace_back([redis_host, redis_port]() {
        RedisConnection redis_conn(redis_host, redis_port);
        process_message_batch(redis_conn);
      });
    }
  } catch (const std::exception &e) {
    std::cerr << "Error starting workers: " << e.what() << std::endl;
    keep_running = false;
    for (auto &consumer : consumers) {
      if (consumer.joinable())
        consumer.join();
    }
    monitor_thread.join();
    return 1;
  }

  // Join all threads
  for (auto &consumer : consumers)
    consumer.join();
  for (auto &worker : workers)
    worker.join();

  // Clean up
  keep_running = false;
  monitor_thread.join();

  return 0;
}
