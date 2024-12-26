#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp> // for JSON parsing
#include <cstdlib>
#include <csignal>

using json = nlohmann::json;

std::atomic<bool> keep_running(true);

void signal_handler(int signum) {
    keep_running = false;
}

void process_message(const std::string &message, const std::string &consumer_id, redisContext *redis_ctx) {
    try {
        json msg_json = json::parse(message);
        msg_json["processed_by"] = consumer_id;

        std::string xadd_command = "XADD messages:processed * message_id " + msg_json["message_id"].get<std::string>() + " processed_by " + consumer_id;
        redisReply *reply = (redisReply *)redisCommand(redis_ctx, xadd_command.c_str());
        if (reply == nullptr) {
            std::cerr << "Error: XADD command failed." << std::endl;
            return;
        }
        freeReplyObject(reply);
    } catch (const std::exception &e) {
        std::cerr << "Error processing message: " << e.what() << std::endl;
    }
}

void consumer_thread(const std::string &consumer_id, const std::string &channel, const std::string &redis_host, int redis_port) {
    redisContext *redis_ctx = redisConnect(redis_host.c_str(), redis_port);
    if (redis_ctx == nullptr || redis_ctx->err) {
        std::cerr << "Error: Unable to connect to Redis." << std::endl;
        if (redis_ctx) redisFree(redis_ctx);
        return;
    }

    redisReply *reply = (redisReply *)redisCommand(redis_ctx, ("SUBSCRIBE " + channel).c_str());
    if (reply == nullptr) {
        std::cerr << "Error: SUBSCRIBE command failed." << std::endl;
        redisFree(redis_ctx);
        return;
    }
    freeReplyObject(reply);

    while (keep_running) {
        redisReply *reply = nullptr;
        if (redisGetReply(redis_ctx, (void **)&reply) == REDIS_OK) {
            if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 3) {
                std::string message = reply->element[2]->str;
                process_message(message, consumer_id, redis_ctx);
            }
            freeReplyObject(reply);
        } else {
            std::cerr << "Error: Failed to get reply from Redis." << std::endl;
            break;
        }
    }

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
    
    std::vector<std::thread> consumers;
    for (int i = 0; i < consumer_count; ++i) {
        consumers.emplace_back(consumer_thread, "consumer_" + std::to_string(i + 1), channel, redis_host, redis_port);
    }

    for (auto &thread : consumers) {
        thread.join();
    }

    return 0;
}
