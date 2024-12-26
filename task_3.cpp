#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>
#include <nlohmann/json.hpp> // for JSON parsing
#include <cstdlib>
#include <csignal>
#include <event2/event.h>

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

void on_message(redisAsyncContext *ctx, void *reply, void *privdata) {
    if (reply == nullptr) return;
    redisReply *redis_reply = (redisReply *)reply;

    if (redis_reply->type == REDIS_REPLY_ARRAY && redis_reply->elements == 3) {
        std::string consumer_id = *(std::string *)privdata;
        std::string message = redis_reply->element[2]->str;
        process_message(message, consumer_id, ctx->c);
    }
}

void consumer_thread(const std::string &consumer_id, const std::string &channel, const std::string &redis_host, int redis_port) {
    struct event_base *base = event_base_new();
    redisAsyncContext *redis_ctx = redisAsyncConnect(redis_host.c_str(), redis_port);

    if (redis_ctx == nullptr || redis_ctx->err) {
        std::cerr << "Error: Unable to connect to Redis asynchronously." << std::endl;
        if (redis_ctx) redisAsyncFree(redis_ctx);
        return;
    }

    redisLibeventAttach(redis_ctx, base);
    redisAsyncCommand(redis_ctx, nullptr, nullptr, "SUBSCRIBE %s", channel.c_str());
    redisAsyncCommand(redis_ctx, on_message, (void *)&consumer_id, "SUBSCRIBE %s", channel.c_str());

    event_base_dispatch(base);

    redisAsyncFree(redis_ctx);
    event_base_free(base);
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
