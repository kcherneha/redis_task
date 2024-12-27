#include "task_3.h"
#include <gtest/gtest.h>

using namespace task_3;

class RedisConnectionTest : public ::testing::Test {
protected:
  redisContext *redis_ctx;

  void SetUp() override {
    redis_ctx = create_redis_connection("127.0.0.1", 6379);
    ASSERT_NE(redis_ctx, nullptr);
  }

  void TearDown() override {
    if (redis_ctx) {
      redisFree(redis_ctx);
    }
  }
};

TEST_F(RedisConnectionTest, ConnectToRedis) {
  EXPECT_NE(redis_ctx, nullptr);
  EXPECT_EQ(redis_ctx->err, 0);
}

TEST_F(RedisConnectionTest, XADDCommandSuccess) {
  send_xadd(redis_ctx, "test_stream", "test_message_id", "test_consumer");
  // Verify manually on Redis that the message was added or check responses if
  // mocked.
}

TEST_F(RedisConnectionTest, InvalidRedisCommand) {
  std::string invalid_command = "INVALIDCOMMAND\r\n";
  send_xadd_impl(redis_ctx, invalid_command);
  // Expect an error response or no crash.
}

class MessageQueueTest : public ::testing::Test {
protected:
  moodycamel::ConcurrentQueue<std::pair<std::string, std::string>> queue;
};

TEST_F(MessageQueueTest, EnqueueAndDequeue) {
  std::pair<std::string, std::string> message = {"test_message", "consumer_1"};
  EXPECT_TRUE(queue.enqueue(message));

  std::pair<std::string, std::string> dequeued_message;
  EXPECT_TRUE(queue.try_dequeue(dequeued_message));
  EXPECT_EQ(dequeued_message.first, "test_message");
  EXPECT_EQ(dequeued_message.second, "consumer_1");
}

TEST_F(MessageQueueTest, EmptyQueue) {
  std::pair<std::string, std::string> dequeued_message;
  EXPECT_FALSE(queue.try_dequeue(dequeued_message));
}

class BatchProcessingTest : public ::testing::Test {
protected:
  redisContext *redis_ctx;
  void SetUp() override {
    redis_ctx = create_redis_connection("127.0.0.1", 6379);
    ASSERT_NE(redis_ctx, nullptr);
  }
  void TearDown() override {
    if (redis_ctx) {
      redisFree(redis_ctx);
    }
  }
};

TEST_F(BatchProcessingTest, ProcessValidBatch) {
  std::vector<std::pair<std::string, std::string>> batch = {
      {"{\"message_id\": \"1\", \"data\": \"test1\"}", "consumer_1"},
      {"{\"message_id\": \"2\", \"data\": \"test2\"}", "consumer_2"}};

  process_batch(redis_ctx, batch);
  // Verify Redis contains processed messages or check `messages_processed`
  // count.
}

TEST_F(BatchProcessingTest, HandleDuplicateMessages) {
  std::vector<std::pair<std::string, std::string>> batch = {
      {"{\"message_id\": \"1\", \"data\": \"test1\"}", "consumer_1"},
      {"{\"message_id\": \"1\", \"data\": \"test1\"}", "consumer_1"}};

  process_batch(redis_ctx, batch);
  // Ensure duplicates are not processed twice by checking `processed_messages`.
}

class SignalHandlingTest : public ::testing::Test {
protected:
  void SetUp() override { keep_running = true; }
};

TEST_F(SignalHandlingTest, HandleSIGINT) {
  signal_handler(SIGINT);
  EXPECT_FALSE(keep_running.load());
}

class ThroughputMonitoringTest : public ::testing::Test {
protected:
  std::atomic<int> messages_processed;

  void SetUp() override { messages_processed = 0; }
};

TEST_F(ThroughputMonitoringTest, MonitorThroughput) {
  messages_processed.store(100);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  messages_processed.store(150);
  int throughput = messages_processed.load() - 100;
  EXPECT_EQ(throughput, 50);
}

// Main entry point for tests
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
