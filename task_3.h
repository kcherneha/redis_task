#pragma once

#include "concurrentqueue.h"
#include <atomic>
#include <hiredis/hiredis.h>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;

namespace task_3 {

// Constants
constexpr size_t BUFFER_SIZE = 4096;
constexpr size_t BATCH_SIZE = 100;

// Global Variables
extern std::atomic<bool> keep_running;
extern std::atomic<int> messages_processed;
extern moodycamel::ConcurrentQueue<std::pair<std::string, std::string>>
    message_queue;

class RedisConnection {
public:
  RedisConnection(const std::string &host, int port) {
    redis_ctx_ = redisConnect(host.c_str(), port);
    if (!redis_ctx_ || redis_ctx_->err) {
      throw std::runtime_error(
          "Error: Unable to connect to Redis - " +
          std::string(redis_ctx_ ? redis_ctx_->errstr : "Connection failed"));
    }
    std::cerr << "Connected to Redis at " << host << ":" << port << std::endl;
  }

  ~RedisConnection() {
    if (redis_ctx_) {
      redisFree(redis_ctx_);
    }
  }

  redisContext *get() const { return redis_ctx_; }

private:
  redisContext *redis_ctx_;
};

void signal_handler(int signum);

void monitor_throughput();

// Redis message sending
void send_xadd(redisContext *redis_ctx, const std::string &stream,
               const std::string &message_id, const std::string &processed_by);

// Message processing
std::vector<std::pair<std::string, std::string>> fetch_message_batch();
void process_batch(
    redisContext *redis_ctx,
    const std::vector<std::pair<std::string, std::string>> &batch);
void process_message_batch(RedisConnection &redis_conn);

void consumer_thread(const std::string &consumer_id, const std::string &channel,
                     const std::string &redis_host, int redis_port);
} // namespace task_3
