#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <unordered_set>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <hiredis/hiredis.h>
#include <hiredis/read.h>
#include <nlohmann/json.hpp> // for JSON parsing
#include <cstdlib>
#include <csignal>
#include <cstring>
#include <unistd.h> // for read(), write()

using json = nlohmann::json;

std::atomic<bool> keep_running(true);
std::unordered_set<std::string> processed_messages;
std::mutex processed_messages_mutex;
std::queue<std::pair<std::string, std::string>> message_queue;
std::mutex queue_mutex;
std::condition_variable queue_condition;

void signal_handler(int signum) {
    keep_running = false;
    queue_condition.notify_all();
}

void send_xadd(int socket_fd, const std::string &stream, const std::string &message_id, const std::string &processed_by) {
    std::string xadd_command = "XADD " + stream + " * message_id " + message_id + " processed_by " + processed_by + "\r\n";
    if (write(socket_fd, xadd_command.c_str(), xadd_command.size()) < 0) {
        std::cerr << "Error: Failed to send XADD command." << std::endl;
    }

    // Parse XADD response
    redisReader *reader = redisReaderCreate();
    if (!reader) {
        std::cerr << "Error: Failed to create Redis reader for XADD." << std::endl;
        return;
    }

    char buffer[4096];
    ssize_t bytes_read = read(socket_fd, buffer, sizeof(buffer));
    if (bytes_read > 0) {
        redisReaderFeed(reader, buffer, bytes_read);
        void *reply = nullptr;
        if (redisReaderGetReply(reader, &reply) == REDIS_OK && reply != nullptr) {
            freeReplyObject(reply);
        }
    }
    redisReaderFree(reader);
}

void process_message_batch(int socket_fd) {
    while (keep_running) {
        std::vector<std::pair<std::string, std::string>> batch;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_condition.wait(lock, [] { return !message_queue.empty() || !keep_running; });
            while (!message_queue.empty() && batch.size() < 100) { // Process in batches of 100
                batch.push_back(message_queue.front());
                message_queue.pop();
            }
        }

        for (const auto &[message, consumer_id] : batch) {
            try {
                json msg_json = json::parse(message);
                std::string message_id = msg_json["message_id"].get<std::string>();

                // Check if the message has already been processed
                {
                    std::lock_guard<std::mutex> lock(processed_messages_mutex);
                    if (processed_messages.find(message_id) != processed_messages.end()) {
                        std::cerr << "Message " << message_id << " already processed. Skipping." << std::endl;
                        continue;
                    }
                    processed_messages.insert(message_id);
                }

                msg_json["processed_by"] = consumer_id;
                std::cout << "Processed message: " << msg_json.dump() << std::endl;

                // Use XADD to append to the Redis stream
                send_xadd(socket_fd, "messages:processed", message_id, consumer_id);

            } catch (const std::exception &e) {
                std::cerr << "Error processing message: " << e.what() << std::endl;
            }
        }
    }
}

void consume_messages(int socket_fd, const std::string &consumer_id) {
    redisReader *reader = redisReaderCreate();
    if (!reader) {
        std::cerr << "Error: Failed to create Redis reader." << std::endl;
        return;
    }

    char buffer[4096];
    while (keep_running) {
        ssize_t bytes_read = read(socket_fd, buffer, sizeof(buffer));
        if (bytes_read <= 0) {
            std::cerr << "Error: Failed to read from socket." << std::endl;
            break;
        }

        redisReaderFeed(reader, buffer, bytes_read);

        void *reply = nullptr;
        while (redisReaderGetReply(reader, &reply) == REDIS_OK && reply != nullptr) {
            redisReply *redis_reply = static_cast<redisReply *>(reply);
            if (redis_reply->type == REDIS_REPLY_ARRAY && redis_reply->elements >= 3) {
                std::string message = redis_reply->element[2]->str;
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    message_queue.emplace(message, consumer_id);
                }
                queue_condition.notify_one();
            }
            freeReplyObject(reply);
        }
    }

    redisReaderFree(reader);
}

void consumer_thread(const std::string &consumer_id, const std::string &channel, const std::string &redis_host, int redis_port) {
    redisContext *redis_ctx = redisConnect(redis_host.c_str(), redis_port);
    if (redis_ctx == nullptr || redis_ctx->err) {
        std::cerr << "Error: Unable to connect to Redis." << std::endl;
        if (redis_ctx) redisFree(redis_ctx);
        return;
    }

    int socket_fd = redis_ctx->fd;

    // Send SUBSCRIBE command manually
    std::string subscribe_command = "SUBSCRIBE " + channel + "\r\n";
    if (write(socket_fd, subscribe_command.c_str(), subscribe_command.size()) < 0) {
        std::cerr << "Error: Failed to send SUBSCRIBE command." << std::endl;
        redisFree(redis_ctx);
        return;
    }

    consume_messages(socket_fd, consumer_id);

    redisFree(redis_ctx);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <consumer_count> <redis_host> <redis_port>" << std::endl;
        return 1;
    }

    int consumer_count = std::stoi(argv[1]);
    std::string redis_host = argv[2];
    int redis_port = std::stoi(argv[3]);
    std::string channel = "messages:published";

    signal(SIGINT, signal_handler);

    // Start a thread pool for processing
    int thread_pool_size = std::thread::hardware_concurrency();
    std::vector<std::thread> workers;
    for (int i = 0; i < thread_pool_size; ++i) {
        workers.emplace_back(process_message_batch, redis_port); // Pass redis_port as a placeholder
    }

    // Start consumer threads
    std::vector<std::thread> consumers;
    for (int i = 0; i < consumer_count; ++i) {
        consumers.emplace_back(consumer_thread, "consumer_" + std::to_string(i + 1), channel, redis_host, redis_port);
    }

    for (auto &thread : consumers) {
        thread.join();
    }

    // Notify workers to stop and join them
    queue_condition.notify_all();
    for (auto &worker : workers) {
        worker.join();
    }

    return 0;
}