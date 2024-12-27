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
#include <queue>
#include <thread>
#include <unistd.h> // for read(), write()
#include <unordered_set>
#include <vector>

using json = nlohmann::json;

constexpr size_t BUFFER_SIZE = 4096;
constexpr size_t BATCH_SIZE = 100;

// Global state and synchronization primitives
std::atomic<bool> keep_running(true);
std::atomic<int> messages_processed(0);
std::unordered_set<std::string> processed_messages;
std::mutex processed_messages_mutex;
std::queue<std::pair<std::string, std::string>> message_queue;
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

// Send an XADD command to Redis
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
  std::unique_lock<std::mutex> lock(queue_mutex);
  queue_condition.wait(lock,
                       [] { return !message_queue.empty() || !keep_running; });
  while (!message_queue.empty() && batch.size() < BATCH_SIZE) {
    batch.push_back(message_queue.front());
    message_queue.pop();
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

      {
        std::lock_guard<std::mutex> lock(processed_messages_mutex);
        if (processed_messages.find(message_id) != processed_messages.end()) {
          continue;
        }
        processed_messages.insert(message_id);
      }

      msg_json["processed_by"] = consumer_id;
      std::cout << "Processed message: " << msg_json.dump() << std::endl;

      send_xadd(redis_ctx, "messages:processed", message_id, consumer_id);
      messages_processed.fetch_add(1, std::memory_order_relaxed);

    } catch (const std::exception &e) {
      std::cerr << "Error processing message: " << e.what() << std::endl;
    }
  }
}

// Process a batch of messages
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

  {
    std::lock_guard<std::mutex> lock(queue_mutex);
    message_queue.emplace(message, consumer_id);
  }
  queue_condition.notify_one();
}

// Consume messages from Redis Pub/Sub
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

// Thread for consuming messages
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

  // Start consumer threads for SUBSCRIBE
  std::vector<std::thread> consumers;
  for (int i = 0; i < consumer_count; ++i) {
    consumers.emplace_back(consumer_thread, "consumer_" + std::to_string(i + 1),
                           channel, redis_host, redis_port);
  }

  // Create Redis connection for XADD
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

  // Start worker threads for XADD
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
