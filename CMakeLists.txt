cmake_minimum_required(VERSION 3.10)
project(SentinelDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include/external/spdlog/include)

add_library(sentineldb_core STATIC
    src/kvstore.cpp
    src/command_parser.cpp
    src/wal.cpp
    src/guard.cpp
    src/replay_service.cpp
)

if(UNIX)
    target_link_libraries(sentineldb_core pthread)
endif()

add_executable(sentineldb_cli src/main.cpp)
add_executable(http_server src/http_server.cpp)
add_executable(test_temporal src/test_temporal.cpp)
add_executable(test_wal_temporal src/test_wal_temporal.cpp)

foreach(target sentineldb_cli http_server test_temporal test_wal_temporal)
    target_link_libraries(${target} sentineldb_core)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endforeach()

if(NOT MSVC)
    target_compile_options(sentineldb_core PRIVATE -Wall -Wextra -Wpedantic)
endif()
