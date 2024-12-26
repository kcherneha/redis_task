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
#include <chrono>
#include <cassert> // For testing

using json = nlohmann::json;

std::atomic<bool> keep_running(true);
std::atomic<int> messages_processed(0);
std::unordered_set<std::string> processed_messages;
std::mutex processed_messages_mutex;
std::queue<std::pair<std::string, std::string>> message_queue;
std::mutex queue_mutex;
std::condition_variable queue_condition;

void signal_handler(int signum) {
    keep_running = false;
    queue_condition.notify_all();
}

void monitor_throughput() {
    while (keep_running) {
        int start_count = messages_processed.load();
        std::this_thread::sleep_for(std::chrono::seconds(3));
        int end_count = messages_processed.load();
        std::cout << "Messages processed in last 3 seconds: " << (end_count - start_count) << std::endl;
    }
}

void send_xadd(int socket_fd, const std::string &stream, const std::string &message_id, const std::string &processed_by) {
    std::cout << "Simulating XADD: " << stream << " " << message_id << " processed_by: " << processed_by << std::endl;
    messages_processed.fetch_add(1, std::memory_order_relaxed);
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
    for (int i = 0; i < 5; ++i) {
        std::string mock_message = R"({"message_id": "" + std::to_string(i) + R"(", "data": "Test Data"})";
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            message_queue.emplace(mock_message, consumer_id);
        }
        queue_condition.notify_one();
    }
}

void consumer_thread(const std::string &consumer_id, const std::string &channel, const std::string &redis_host, int redis_port) {
    std::cout << "Simulating consumer: " << consumer_id << " subscribing to channel: " << channel << std::endl;
    consume_messages(redis_port, consumer_id);
}

// Unit Test: Test send_xadd
void test_send_xadd() {
    send_xadd(0, "test_stream", "12345", "test_consumer");
    assert(messages_processed.load() == 1);
    std::cout << "Test passed: send_xadd" << std::endl;
}

// Unit Test: Test process_message_batch
void test_process_message_batch() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        message_queue.emplace(R"({"message_id": "54321", "data": "Batch Test"})", "batch_consumer");
    }
    queue_condition.notify_one();

    process_message_batch(0);
    assert(processed_messages.find("54321") != processed_messages.end());
    std::cout << "Test passed: process_message_batch" << std::endl;
}

// Unit Test: Test consume_messages
void test_consume_messages() {
    consume_messages(0, "mock_consumer");
    assert(!message_queue.empty());
    std::cout << "Test passed: consume_messages" << std::endl;
}

// Unit Test: Test consumer_thread
void test_consumer_thread() {
    std::thread consumer(consumer_thread, "test_consumer", "test_channel", "localhost", 6379);
    consumer.join();
    assert(!message_queue.empty());
    std::cout << "Test passed: consumer_thread" << std::endl;
}

int main() {
    test_send_xadd();
    test_process_message_batch();
    test_consume_messages();
    test_consumer_thread();

    std::cout << "All tests passed." << std::endl;
    return 0;
}
