#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include "kvstore.h"
#include "wal.h"

void printTimestamp(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        tp.time_since_epoch()) % 1000;
    std::cout << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S")
              << "." << std::setfill('0') << std::setw(3) << ms.count();
}

int main() {
    std::cout << "=== Test 1: Create versioned data ===\n";
    
    auto wal1 = std::make_shared<WAL>("data/test_wal.log");
    wal1->initialize();
    
    auto store1 = std::make_shared<KVStore>(wal1);
    
    std::cout << "Setting price=100\n";
    store1->set("price", "100");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    std::cout << "Setting price=150\n";
    store1->set("price", "150");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    std::cout << "Setting price=200\n";
    store1->set("price", "200");
    
    auto history1 = store1->getHistory("price");
    std::cout << "\nHistory before restart (" << history1.size() << " versions):\n";
    for (size_t i = 0; i < history1.size(); ++i) {
        std::cout << "  Version " << (i+1) << ": ";
        printTimestamp(history1[i].timestamp);
        std::cout << " -> \"" << history1[i].value << "\"\n";
    }
    
    // Force WAL flush
    wal1->flush();
    
    std::cout << "\n=== Test 2: Restart and replay ===\n";
    
    // Create new instances (simulating restart)
    auto wal2 = std::make_shared<WAL>("data/test_wal.log");
    wal2->initialize();
    
    auto store2 = std::make_shared<KVStore>(wal2);
    
    // Manually replay (like main.cpp does)
    store2->setWalEnabled(false);
    std::vector<std::string> commands = wal2->readLog();
    std::cout << "Replaying " << commands.size() << " commands from WAL\n";
    
    for (const auto& cmdLine : commands) {
        std::istringstream iss(cmdLine);
        std::string cmdType, key, value;
        long long timestampMs;
        
        iss >> cmdType >> std::quoted(key) >> std::quoted(value) >> timestampMs;
        
        if (cmdType == "SET") {
            auto timestamp = std::chrono::system_clock::time_point(
                std::chrono::milliseconds(timestampMs));
            store2->setAtTime(key, value, timestamp);
        }
    }
    store2->setWalEnabled(true);
    
    auto history2 = store2->getHistory("price");
    std::cout << "\nHistory after restart (" << history2.size() << " versions):\n";
    for (size_t i = 0; i < history2.size(); ++i) {
        std::cout << "  Version " << (i+1) << ": ";
        printTimestamp(history2[i].timestamp);
        std::cout << " -> \"" << history2[i].value << "\"\n";
    }
    
    std::cout << "\n=== Test 3: Verify timestamps match ===\n";
    bool timestampsMatch = true;
    if (history1.size() == history2.size()) {
        for (size_t i = 0; i < history1.size(); ++i) {
            auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
                history1[i].timestamp - history2[i].timestamp).count();
            if (diff != 0) {
                std::cout << "Version " << (i+1) << " timestamp differs by " << diff << "ms\n";
                timestampsMatch = false;
            }
        }
        if (timestampsMatch) {
            std::cout << "✓ All timestamps match perfectly!\n";
        }
    } else {
        std::cout << "✗ Version count mismatch\n";
        timestampsMatch = false;
    }
    
    std::cout << "\n=== Test 4: Current value ===\n";
    auto current = store2->get("price");
    std::cout << "GET price: " << (current ? *current : "(nil)") << "\n";
    
    std::cout << "\n=== Test Complete ===\n";
    return timestampsMatch ? 0 : 1;
}
