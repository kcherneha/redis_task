#pragma once

#include "concurrentqueue.h"
#include <atomic>
#include <hiredis/hiredis.h>
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
extern std::unordered_set<std::string> processed_messages;
extern std::mutex processed_messages_mutex;
extern std::mutex cout_mutex;
extern moodycamel::ConcurrentQueue<std::pair<std::string, std::string>>
    message_queue;

// Signal handling
void signal_handler(int signum);

// Throughput monitoring
void monitor_throughput();

// Redis connection management
redisContext *create_redis_connection(const std::string &redis_host,
                                      int redis_port);
void send_xadd_impl(redisContext *redis_ctx, const std::string &command);
void send_xadd(redisContext *redis_ctx, const std::string &stream,
               const std::string &message_id, const std::string &processed_by);

// Message processing
std::vector<std::pair<std::string, std::string>> fetch_message_batch();
void process_batch(
    redisContext *redis_ctx,
    const std::vector<std::pair<std::string, std::string>> &batch);
void process_message_batch(redisContext *redis_ctx);

// Message consumption
void parse_redis_reply(redisReply *reply, const std::string &consumer_id);
void consume_messages(redisContext *redis_ctx, const std::string &consumer_id);
void consumer_thread(const std::string &consumer_id, const std::string &channel,
                     const std::string &redis_host, int redis_port);
} // namespace task_3
