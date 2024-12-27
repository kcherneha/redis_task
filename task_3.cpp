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
moodycamel::ConcurrentQueue<std::pair<std::string, std::string>> message_queue;

void monitor_throughput() {
  while (keep_running) {
    std::this_thread::sleep_for(std::chrono::seconds(2));
    int start_count = messages_processed.load();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int end_count = messages_processed.load();
    std::cout << "Messages processed per last second: "
              << end_count - start_count << std::endl;
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
  redisReply *reply =
      static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
  if (reply) {
    if (reply->type == REDIS_REPLY_ERROR) {
      std::cerr << "Error: XADD command failed: " << reply->str << std::endl;
    }
    freeReplyObject(reply);
  } else {
    std::cerr << "Error: Failed to execute XADD command." << std::endl;
  }
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
  for (const auto &[message, consumer_id] : batch) {
    try {
      json msg_json = json::parse(message);
      std::string message_id = msg_json["message_id"].get<std::string>();
      send_xadd(redis_ctx, "messages:processed", message_id, consumer_id);
      messages_processed.fetch_add(1, std::memory_order_relaxed);
    } catch (const std::exception &e) {
      std::cerr << "Error processing message: " << e.what() << std::endl;
    }
  }
}

void process_message_batch(RedisConnection &redis_conn) {
  while (keep_running) {
    auto batch = fetch_message_batch();
    process_batch(redis_conn.get(), batch);
  }
}

void consumer_thread(const std::string &consumer_id, const std::string &channel,
                     const std::string &redis_host, int redis_port) {
  RedisConnection redis_conn(redis_host, redis_port);
  int socket_fd = redis_conn.get()->fd;
  std::string subscribe_command = "SUBSCRIBE " + channel + "\r\n";
  if (write(socket_fd, subscribe_command.c_str(), subscribe_command.size()) <
      0) {
    std::cerr << "Error: Failed to send SUBSCRIBE command." << std::endl;
    return;
  }

  redisReader *reader = redisReaderCreate();
  if (!reader) {
    std::cerr << "Error: Failed to create Redis reader." << std::endl;
    return;
  }

  char buffer[BUFFER_SIZE];
  while (keep_running) {
    ssize_t bytes_read = read(socket_fd, buffer, sizeof(buffer));
    if (bytes_read <= 0) {
      std::cerr << "Error: Failed to read from socket." << std::endl;
      break;
    }
    redisReaderFeed(reader, buffer, bytes_read);
    void *reply = nullptr;
    while (redisReaderGetReply(reader, &reply) == REDIS_OK &&
           reply != nullptr) {
      auto *redis_reply = static_cast<redisReply *>(reply);
      if (redis_reply->type == REDIS_REPLY_ARRAY &&
          redis_reply->elements >= 3) {
        redisReply *message_element = redis_reply->element[2];
        if (message_element->type == REDIS_REPLY_STRING) {
          std::string message(message_element->str, message_element->len);
          message_queue.enqueue({message, consumer_id});
        }
      }
      freeReplyObject(reply);
    }
  }
  redisReaderFree(reader);
}

} // namespace task_3
