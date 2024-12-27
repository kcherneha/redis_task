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

// Use Redis Set for processed messages
bool is_message_processed(redisContext *redis_ctx,
                          const std::string &message_id) {
  std::string command = "SISMEMBER processed_messages " + message_id + "\r\n";
  redisReply *reply =
      static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
  if (reply) {
    bool result = reply->integer == 1;
    freeReplyObject(reply);
    return result;
  }
  return false;
}

void mark_message_as_processed(redisContext *redis_ctx,
                               const std::string &message_id) {
  std::string command = "SADD processed_messages " + message_id + "\r\n";
  redisReply *reply =
      static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
  if (reply) {
    freeReplyObject(reply);
  }
}

// Use Redis List for message queue
void enqueue_message(redisContext *redis_ctx, const std::string &message,
                     const std::string &consumer_id) {
  std::string command =
      "LPUSH messages_queue " + message + " " + consumer_id + "\r\n";
  redisReply *reply =
      static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
  if (reply) {
    freeReplyObject(reply);
  }
}

std::pair<std::string, std::string> dequeue_message(redisContext *redis_ctx) {
  std::string command = "RPOP messages_queue\r\n";
  redisReply *reply =
      static_cast<redisReply *>(redisCommand(redis_ctx, command.c_str()));
  if (reply && reply->type == REDIS_REPLY_STRING) {
    std::string message(reply->str, reply->len);
    freeReplyObject(reply);
    return {message, ""}; // Include consumer_id handling if needed
  }
  return {"", ""};
}

std::vector<std::pair<std::string, std::string>>
fetch_message_batch(redisContext *redis_ctx) {
  std::vector<std::pair<std::string, std::string>> batch;
  for (size_t i = 0; i < BATCH_SIZE; ++i) {
    auto message = dequeue_message(redis_ctx);
    if (!message.first.empty()) {
      batch.push_back(message);
    } else {
      break;
    }
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

      if (is_message_processed(redis_ctx, message_id)) {
        continue;
      }
      mark_message_as_processed(redis_ctx, message_id);

      msg_json["processed_by"] = consumer_id;
      std::cout << "Processed message: " << msg_json.dump() << std::endl;

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
    auto batch = fetch_message_batch(redis_ctx);
    process_batch(redis_ctx, batch);
  }
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
      redisReply *redis_reply = static_cast<redisReply *>(reply);
      if (!redis_reply || redis_reply->type != REDIS_REPLY_ARRAY ||
          redis_reply->elements < 3) {
        std::cerr << "Error: Invalid Redis reply structure." << std::endl;
        if (reply)
          freeReplyObject(reply);
        continue;
      }

      redisReply *message_element = redis_reply->element[2];
      if (!message_element || message_element->type != REDIS_REPLY_STRING) {
        std::cerr << "Error: Redis reply element[2] is null or not a string."
                  << std::endl;
        freeReplyObject(reply);
        continue;
      }

      std::string message(message_element->str, message_element->len);
      enqueue_message(redis_ctx, message, consumer_id);
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

  consume_messages(redis_ctx, consumer_id);
  redisFree(redis_ctx);
}

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
