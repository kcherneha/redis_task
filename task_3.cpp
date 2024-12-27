#include "task_3.h"
#include "concurrentqueue.h" // External library for lock-free queue
#include <atomic>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <thread>
#include <unistd.h> // for read(), write()
#include <unordered_set>
#include <vector>

using json = nlohmann::json;

namespace task_3 {

std::atomic<bool> keep_running(true);
std::atomic<int> messages_processed(0);
std::unordered_set<std::string> processed_messages;
std::mutex processed_messages_mutex;
std::mutex cout_mutex;
moodycamel::ConcurrentQueue<std::pair<std::string, std::string>>
    message_queue; // Lock-free queue

void signal_handler(int signum) {
  keep_running = false;
  std::cerr << "Signal handler triggered. Shutting down..." << std::endl;
}

void monitor_throughput() {
  while (keep_running) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    int start_count = messages_processed.load();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int end_count = messages_processed.load();
    {
      std::lock_guard<std::mutex> lock(cout_mutex);
      std::cout << "Messages processed per last second: "
                << end_count - start_count << std::endl;
    }
  }
}

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
  std::cerr << "Connected to Redis at " << redis_host << ":" << redis_port
            << std::endl;
  return redis_ctx;
}

void send_xadd_impl(redisContext *redis_ctx, const std::string &command) {
  if (write(redis_ctx->fd, command.c_str(), command.size()) < 0) {
    std::cerr << "Error: Failed to send XADD command." << std::endl;
    return;
  }

  char buffer[BUFFER_SIZE];
  ssize_t bytes_read = read(redis_ctx->fd, buffer, sizeof(buffer));
  if (bytes_read > 0) {
    redisReply *reply =
        static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
    if (reply) {
      if (reply->type == REDIS_REPLY_ERROR) {
        std::cerr << "Error: XADD command failed: " << reply->str << std::endl;
      } else {
        // std::cout << "XADD command succeeded: " << command << std::endl;
      }
      freeReplyObject(reply);
    }
  } else {
    std::cerr << "Error: Failed to read response for XADD." << std::endl;
  }
}

void send_xadd(redisContext *redis_ctx, const std::string &stream,
               const std::string &message_id, const std::string &processed_by) {
  if (!redis_ctx) {
    std::cerr << "Error: Redis connection is null for XADD." << std::endl;
    return;
  }
  std::string command = "XADD " + stream + " * message_id " + message_id +
                        " processed_by " + processed_by + "\r\n";
  send_xadd_impl(redis_ctx, command);
}

std::vector<std::pair<std::string, std::string>> fetch_message_batch() {
  std::vector<std::pair<std::string, std::string>> batch;
  for (size_t i = 0; i < BATCH_SIZE && keep_running; ++i) {
    std::pair<std::string, std::string> item;
    if (message_queue.try_dequeue(item)) {
      batch.push_back(item);
    } else {
      break;
    }
  }
  if (!batch.empty()) {
    std::cout << "Fetched batch of size: " << batch.size() << std::endl;
  }
  return batch;
}

void process_batch(
    redisContext *redis_ctx,
    const std::vector<std::pair<std::string, std::string>> &batch) {
  if (!batch.empty()) {
    std::cout << "Worker started processing batch of size: " << batch.size()
              << std::endl;
  }
  for (const auto &[message, consumer_id] : batch) {
    try {
      json msg_json = json::parse(message);
      std::string message_id = msg_json["message_id"].get<std::string>();
      {
        std::lock_guard<std::mutex> lock(processed_messages_mutex);
        if (processed_messages.find(message_id) != processed_messages.end()) {
          // std::cout << "Duplicate message detected: " << message_id
          //           << ". Skipping." << std::endl;
          continue;
        }
        processed_messages.insert(message_id);
      }
      msg_json["processed_by"] = consumer_id;
      // std::cout << "Processed message: " << msg_json.dump() << std::endl;
      send_xadd(redis_ctx, "messages:processed", message_id, consumer_id);
      messages_processed.fetch_add(1, std::memory_order_relaxed);
    } catch (const std::exception &e) {
      std::cerr << "Error processing message: " << e.what() << std::endl;
    }
  }
  // std::cout << "Worker finished processing batch." << std::endl;
}

void process_message_batch(redisContext *redis_ctx) {
  while (keep_running) {
    auto batch = fetch_message_batch();
    process_batch(redis_ctx, batch);
  }
}

void parse_redis_reply(redisReply *reply, const std::string &consumer_id) {
  if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements < 3) {
    std::cerr << "Error: Invalid Redis reply structure." << std::endl;
    return;
  }
  redisReply *message_element = reply->element[2];
  if (!message_element || message_element->type != REDIS_REPLY_STRING) {
    std::cerr << "Error: Redis reply element[2] is null or not a string."
              << std::endl;
    return;
  }
  std::string message(message_element->str, message_element->len);
  if (message.empty()) {
    std::cerr << "Error: Empty message received." << std::endl;
    return;
  }
  if (!message_queue.enqueue({message, consumer_id})) {
    std::cerr << "Error: Failed to enqueue message." << std::endl;
    return;
  }
  // std::cout << "Consumer " << consumer_id << " enqueued message: " << message
  //           << std::endl;
}

void consume_messages(redisContext *redis_ctx, const std::string &consumer_id) {
  if (!redis_ctx) {
    std::cerr << "Error: Redis connection is null for SUBSCRIBE." << std::endl;
    return;
  }
  redisReader *reader = redisReaderCreate();
  if (!reader) {
    std::cerr << "Error: Failed to create Redis reader." << std::endl;
    return;
  }
  char buffer[BUFFER_SIZE];
  while (keep_running) {
    ssize_t bytes_read = read(redis_ctx->fd, buffer, sizeof(buffer));
    if (bytes_read <= 0) {
      std::cerr << "Error: Failed to read from socket." << std::endl;
      break;
    }
    redisReaderFeed(reader, buffer, bytes_read);
    void *reply = nullptr;
    while (redisReaderGetReply(reader, &reply) == REDIS_OK &&
           reply != nullptr) {
      parse_redis_reply(static_cast<redisReply *>(reply), consumer_id);
      freeReplyObject(reply);
    }
  }
  redisReaderFree(reader);
}

void consumer_thread(const std::string &consumer_id, const std::string &channel,
                     const std::string &redis_host, int redis_port) {
  redisContext *redis_ctx = create_redis_connection(redis_host, redis_port);
  if (!redis_ctx)
    return;
  int socket_fd = redis_ctx->fd;
  std::string subscribe_command = "SUBSCRIBE " + channel + "\r\n";
  if (write(socket_fd, subscribe_command.c_str(), subscribe_command.size()) <
      0) {
    std::cerr << "Error: Failed to send SUBSCRIBE command." << std::endl;
    redisFree(redis_ctx);
    return;
  }
  std::cout << "Consumer " << consumer_id
            << " subscribed to channel: " << channel << std::endl;
  consume_messages(redis_ctx, consumer_id);
  redisFree(redis_ctx);
}
} // namespace task_3