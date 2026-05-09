#ifndef WAL_H
#define WAL_H

#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include "status.h"

// Write-Ahead Log manager for persistence
class WAL {
private:
    std::string walPath;
    std::string snapshotPath;
    std::ofstream logFile;
    int logFd_{-1};
    bool enabled;

    // Group commit
    std::thread flushThread_;
    std::mutex flushMutex_;
    std::condition_variable flushCV_;
    bool flushShutdown_{false};
    bool pendingFlush_{false};
    void flushThreadFunc();

public:
    // Constructor with WAL file path
    explicit WAL(const std::string& path);
    
    // Destructor - ensures file is closed
    ~WAL();
    
    // Disable copy constructor and assignment
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    
    // Initialize WAL (create directories, open file)
    Status initialize();
    
    // Log a SET command to WAL with timestamp
    Status logSet(const std::string& key, const std::string& value,
                  std::chrono::system_clock::time_point timestamp);
    
    // Log a DEL command to WAL
    Status logDel(const std::string& key);
    
    // Log a POLICY SET command to WAL
    Status logPolicy(const std::string& policyName);
    
    // Log a GUARD ADD command to WAL
    Status logGuardAdd(const std::string& guardType, const std::string& guardName,
                       const std::string& keyPattern, const std::string& params);

    // Log an AUDIT event to WAL
    Status logAudit(const std::string& key,
                    const std::string& originalValue,
                    const std::string& finalValue,
                    const std::string& outcome,
                    const std::string& policy,
                    const std::string& guards,
                    const std::string& alternatives,
                    std::chrono::system_clock::time_point timestamp);
    
    // Read all commands from WAL file
    std::vector<std::string> readLog();
    
    // Read snapshot file
    std::vector<std::string> readSnapshot();
    
    // Create snapshot from current state and clear WAL
    Status createSnapshot(const std::unordered_map<std::string, std::string>& data,
                         const std::string& currentPolicy = "",
                         const std::vector<std::string>& guardCommands = {},
                         const std::string& retentionCommand = "");
    
    // Check if WAL is enabled and working
    bool isEnabled() const;
    
    // Flush pending writes to disk
    void flush();
    
private:
    static uint32_t computeCRC32(const std::string& data);

    // Create directory if it doesn't exist
    bool createDirectory(const std::string& path);
    
    // Check if file exists
    bool fileExists(const std::string& path);
    
    // Clear the WAL log file
    Status clearLog();
};

#endif // WAL_H
