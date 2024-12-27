## Repository

This code is hosted on GitHub:
[https://github.com/kcherneha/redis_task](https://github.com/kcherneha/redis_task)

## Requirements

### Prerequisites

- **Redis**: Ensure Redis is installed and running: for that task you need to run redis-server and provided Python script for publisher.
- **C++ Compiler**: A modern C++ compiler with C++17 support (e.g., GCC 7+, Clang 6+, or MSVC 2017+).
- **GoogleTest**: Installed or built from source (instructions below).

### Dependencies

- [hiredis](https://github.com/redis/hiredis): For Redis communication.
brew install hiredis

- [ConcurrentQueue](https://github.com/cameron314/concurrentqueue): For lock-free thread-safe queuing.
header-only lib, already included in this repo

- [nlohmann/json](https://github.com/nlohmann/json): For JSON handling.
brew install nlohmann-json

## Build Instructions

All was built and tested on MacOS M1 Sonoma 14.0 only.

### Clone the Repository
```bash
git clone https://github.com/kcherneha/redis_task.git
cd redis_task
```

### Build the Project

Use `g++` to compile the project. The following command builds the main executable (example for macos only):

```bash
g++ -std=c++17 -o consumer_queue task_3.cpp task_3_main.cpp -I/opt/homebrew/include -I/opt/homebrew/Cellar/hiredis/1.2.0/include -L/opt/homebrew/Cellar/hiredis/1.2.0/lib -lhiredis -lpthread
```

- `-std=c++17`: Enables C++17 features.
- `-lpthread`: Links the pthread library for multithreading.
- `-lhiredis`: Links the hiredis library for Redis communication.

### Run the Application

```bash
./consumer_queue <consumer_count> <redis_host> <redis_port>
```

- `consumer_count`: Number of consumer threads to process messages.
- `redis_host`: The Redis server's hostname (e.g., `localhost`).
- `redis_port`: The Redis server's port (e.g., `6379`).

Example:
```bash
./consumer_queue 4 127.0.0.1 6379
```

## Testing

### Build and Run Tests

To build and run the GoogleTest-based test suite:

1. Ensure GoogleTest is installed or built from source.
   - **Build from Source:**
     ```bash
     git clone https://github.com/google/googletest.git
     cd googletest
     mkdir build && cd build
     cmake ..
     make
     sudo make install
     ```

2. Build the tests using `g++`:
   ```bash
   g++ -std=c++17 -o redis_tests task_4_tests.cpp task_3.cpp -I/path/to/googletest/googletest/include -I/opt/homebrew/include -I/opt/homebrew/Cellar/hiredis/1.2.0/include -L/opt/homebrew/Cellar/hiredis/1.2.0/lib -L/path/to/googletest/build/lib -lgtest -lgtest_main -lhiredis -lpthread
   ```

3. Run the tests:
   ```bash
   ./redis_tests
   ```

## Possible improvements

XREADGROUP usage and some advanced stream processing (need to read more documentation and examples), async operations without redundant connections creation and overall non-blocking event driven approach. 
I guess monitoring can be achieved by redis functionality, as well as message queue processing, but I need to investigate this. Also, deduplication process can be changed on redis SET, probably, to keep only unique values.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

