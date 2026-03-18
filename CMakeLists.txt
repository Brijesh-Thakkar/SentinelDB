cmake_minimum_required(VERSION 3.10)
project(RedisLikeDB)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include/external/spdlog/include)

# Source files
set(SOURCES
    src/main.cpp
    src/kvstore.cpp
    src/command_parser.cpp
    src/wal.cpp
    src/guard.cpp
)

# Create executable
add_executable(redis_db ${SOURCES})

# Create test executable for temporal features
add_executable(test_temporal 
    src/test_temporal.cpp
    src/kvstore.cpp
    src/wal.cpp
    src/guard.cpp
)

# Create test executable for temporal WAL
add_executable(test_wal_temporal
    src/test_wal_temporal.cpp
    src/kvstore.cpp
    src/wal.cpp
    src/guard.cpp
)

# Create HTTP server executable
add_executable(http_server
    src/http_server.cpp
    src/kvstore.cpp
    src/wal.cpp
    src/guard.cpp
)

# Link pthread for http_server (required by cpp-httplib)
if(UNIX)
    target_link_libraries(http_server pthread)
endif()

# Enable warnings
if(MSVC)
    target_compile_options(redis_db PRIVATE /W4)
    target_compile_options(http_server PRIVATE /W4)
else()
    target_compile_options(redis_db PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(http_server PRIVATE -Wall -Wextra -Wpedantic)
endif()
