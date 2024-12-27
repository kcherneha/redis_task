#include <atomic>
#include <cassert> // For testing
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp> // for JSON parsing
#include <thread>
#include <unistd.h> // for read(), write()
#include <vector>

using json = nlohmann::json;

constexpr size_t BUFFER_SIZE = 4096;
constexpr size_t BATCH_SIZE = 100;

// Global state and synchronization primitives
std::atomic<bool> keep_running(true);
std::atomic<int> messages_processed(0);
std::mutex queue_mutex;
std::condition_variable queue_condition;

// Signal handler
void signal_handler(int signum) {
  keep_running = false;
  queue_condition.notify_all();
}

// Monitor throughput of processed messages
void monitor_throughput() {
  while (keep_running) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    int start_count = messages_processed.load();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int end_count = messages_processed.load();
    std::cout << "Messages processed per last second: "
              << (end_count - start_count) << std::endl;
  }
}

// Create Redis connection
redisContext *create_redis_connection(const std::string &redis_host,
                                      int redis_port) {
  redisContext *redis_ctx = redisConnect(redis_host.c_str(), redis_port);
  if (!redis_ctx || redis_ctx->err) {
    std::cerr << "Error: Unable to connect to Redis - "
              << (redis_ctx ? redis_ctx->errstr : "Connection failed")
              << std::endl;
    if (redis_ctx)
      redisFree(redis_ctx);
    return nullptr;
  }
  return redis_ctx;
}

// Consume messages using Redis Streams and Consumer Groups
void consume_stream_messages(redisContext *redis_ctx, const std::string &group,
                             const std::string &consumer) {
  while (keep_running) {
    std::string command = "XREADGROUP GROUP " + group + " " + consumer +
                          " COUNT " + std::to_string(BATCH_SIZE) +
                          " BLOCK 1000 STREAMS messages:published >\r\n";
    redisReply *reply =
        static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));

    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
      if (reply)
        freeReplyObject(reply);
      continue;
    }

    for (size_t i = 0; i < reply->elements; ++i) {
      redisReply *message = reply->element[i];
      if (message && message->type == REDIS_REPLY_ARRAY &&
          message->elements >= 2) {
        std::string message_id = message->element[0]->str;
        std::string message_body = message->element[1]->str;

        std::string enqueue_command =
            "LPUSH messages_queue " + message_body + " " + consumer + "\r\n";
        redisReply *enqueue_reply = static_cast<redisReply *>(
            redisCommand(redis_ctx, enqueue_command.c_str()));
        if (enqueue_reply)
          freeReplyObject(enqueue_reply);

        // Acknowledge the message
        std::string ack_command =
            "XACK messages:published " + group + " " + message_id + "\r\n";
        redisReply *ack_reply = static_cast<redisReply *>(
            redisCommand(redis_ctx, ack_command.c_str()));
        if (ack_reply)
          freeReplyObject(ack_reply);
      }
    }
    freeReplyObject(reply);
  }
}

// Process a batch of messages
void process_batch(
    redisContext *redis_ctx,
    const std::vector<std::pair<std::string, std::string>> &batch) {
  for (const auto &[message, consumer_id] : batch) {
    try {
      json msg_json = json::parse(message);
      std::string message_id = msg_json["message_id"].get<std::string>();

      std::string command = "XADD messages:processed * message_id " +
                            message_id + " processed_by " + consumer_id +
                            "\r\n";
      redisReply *reply =
          static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
      if (reply) {
        freeReplyObject(reply);
      }

      messages_processed.fetch_add(1, std::memory_order_relaxed);
    } catch (const std::exception &e) {
      std::cerr << "Error processing message: " << e.what() << std::endl;
    }
  }
}

void process_message_batch(redisContext *redis_ctx) {
  while (keep_running) {
    std::string dequeue_command =
        "LRANGE messages_queue 0 " + std::to_string(BATCH_SIZE - 1) + "\r\n";
    redisReply *reply = static_cast<redisReply *>(
        redisCommand(redis_ctx, dequeue_command.c_str()));
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
      if (reply)
        freeReplyObject(reply);
      continue;
    }

    std::vector<std::pair<std::string, std::string>> batch;
    for (size_t i = 0; i < reply->elements; ++i) {
      std::string message(reply->element[i]->str, reply->element[i]->len);
      batch.emplace_back(message, "worker");
    }

    process_batch(redis_ctx, batch);

    std::string trim_command =
        "LTRIM messages_queue " + std::to_string(BATCH_SIZE) + " -1\r\n";
    redisReply *trim_reply = static_cast<redisReply *>(
        redisCommand(redis_ctx, trim_command.c_str()));
    if (trim_reply)
      freeReplyObject(trim_reply);

    freeReplyObject(reply);
  }
}

int main(int argc, char *argv[]) {
  if (argc < 5) {
    std::cerr << "Usage: " << argv[0]
              << " <consumer_count> <redis_host> <redis_port> <group_name>"
              << std::endl;
    return 1;
  }

  int consumer_count = std::stoi(argv[1]);
  std::string redis_host = argv[2];
  int redis_port = std::stoi(argv[3]);
  std::string group_name = argv[4];

  signal(SIGINT, signal_handler);

  redisContext *setup_ctx = create_redis_connection(redis_host, redis_port);
  if (!setup_ctx)
    return 1;

  std::string group_command =
      "XGROUP CREATE messages:published " + group_name + " $ MKSTREAM\r\n";
  redisReply *group_reply =
      static_cast<redisReply *>(redisCommand(setup_ctx, group_command.c_str()));
  if (group_reply)
    freeReplyObject(group_reply);
  redisFree(setup_ctx);

  std::thread monitor_thread(monitor_throughput);

  std::vector<std::thread> consumers;
  for (int i = 0; i < consumer_count; ++i) {
    consumers.emplace_back(consume_stream_messages,
                           create_redis_connection(redis_host, redis_port),
                           group_name, "consumer_" + std::to_string(i + 1));
  }

  redisContext *xadd_redis_ctx =
      create_redis_connection(redis_host, redis_port);
  if (!xadd_redis_ctx) {
    keep_running = false;
    for (auto &consumer : consumers) {
      consumer.join();
    }
    monitor_thread.join();
    return 1;
  }

  std::vector<std::thread> workers;
  int thread_pool_size = std::thread::hardware_concurrency();
  for (int i = 0; i < thread_pool_size; ++i) {
    workers.emplace_back(process_message_batch, xadd_redis_ctx);
  }

  for (auto &consumer : consumers) {
    consumer.join();
  }

  queue_condition.notify_all();
  for (auto &worker : workers) {
    worker.join();
  }

  redisFree(xadd_redis_ctx);
  keep_running = false;
  monitor_thread.join();

  return 0;
}