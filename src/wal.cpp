#include "wal.h"
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cctype>
#include <iomanip>

WAL::WAL(const std::string& path) : walPath(path), enabled(false) {
    // Derive snapshot path from WAL path
    size_t lastSlash = path.find_last_of("/\\");
    if (lastSlash != std::string::npos) {
        snapshotPath = path.substr(0, lastSlash) + "/snapshot.db";
    } else {
        snapshotPath = "snapshot.db";
    }
}

WAL::~WAL() {
    {
        std::lock_guard<std::mutex> lock(flushMutex_);
        flushShutdown_ = true;
        pendingFlush_ = true;
    }
    flushCV_.notify_all();
    if (flushThread_.joinable()) {
        flushThread_.join();
    }

    if (logFile.is_open()) {
        try {
            logFile.flush();
            logFile.close();
            if (logFd_ != -1) {
                ::close(logFd_);
                logFd_ = -1;
            }
        } catch (...) {
            // Silently handle errors during destruction
        }
    }
}

// Simple CRC32 implementation (no external dependencies)
uint32_t WAL::computeCRC32(const std::string& data) {
    uint32_t crc = 0xFFFFFFFF;
    for (unsigned char byte : data) {
        crc ^= byte;
        for (int i = 0; i < 8; i++) {
            uint32_t mask = -(crc & 1);
            crc = (crc >> 1) ^ (0xEDB88320 & mask);
        }
    }
    return crc ^ 0xFFFFFFFF;
}

void WAL::flushThreadFunc() {
    while (true) {
        std::unique_lock<std::mutex> lock(flushMutex_);
        flushCV_.wait_for(lock, std::chrono::milliseconds(5),
            [this] { return pendingFlush_ || flushShutdown_; });

        if (flushShutdown_ && !pendingFlush_) break;

        if (pendingFlush_) {
            pendingFlush_ = false;
            lock.unlock();
            if (logFd_ != -1) {
                ::fsync(logFd_);
            }
        }
    }
}

Status WAL::initialize() {
    try {
        // Extract directory path from file path
        size_t lastSlash = walPath.find_last_of("/\\");
        if (lastSlash != std::string::npos) {
            std::string dirPath = walPath.substr(0, lastSlash);
            if (!createDirectory(dirPath)) {
                std::cerr << "Warning: Failed to create WAL directory: " << dirPath << "\n";
                enabled = false;
                return Status::ERROR;
            }
        }
        
        // Open file in append mode
        logFile.open(walPath, std::ios::app);
        if (!logFile.is_open()) {
            std::cerr << "Warning: Failed to open WAL file: " << walPath << "\n";
            std::cerr << "Continuing without WAL (data will not persist)\n";
            enabled = false;
            return Status::ERROR;
        }
        logFd_ = ::open(walPath.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        flushShutdown_ = false;
        pendingFlush_ = false;
        flushThread_ = std::thread(&WAL::flushThreadFunc, this);
        
        enabled = true;
        std::cout << "WAL initialized: " << walPath << "\n";
        return Status::OK;
        
    } catch (const std::exception& e) {
        std::cerr << "Warning: WAL initialization error: " << e.what() << "\n";
        std::cerr << "Continuing without WAL (data will not persist)\n";
        enabled = false;
        return Status::ERROR;
    }
}

Status WAL::logSet(const std::string& key, const std::string& value,
                   std::chrono::system_clock::time_point timestamp) {
    if (!enabled || !logFile.is_open()) {
        return Status::ERROR;
    }
    
    try {
        // Convert timestamp to epoch milliseconds
        auto epochMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            timestamp.time_since_epoch()).count();
        
        // Format: SET "key" "value" timestamp_ms
        std::ostringstream contentStream;
        contentStream << "SET " << std::quoted(key) << " " << std::quoted(value)
                      << " " << epochMs;
        std::string content = contentStream.str();
        uint32_t crc = computeCRC32(content);
        logFile << content << " CRC:" << std::hex << std::setw(8) << std::setfill('0')
            << crc << std::dec << std::setfill(' ') << "\n";
        logFile.flush(); // Ensure it's written to disk
        // Group commit: signal background thread to fsync within 5ms
        {
            std::lock_guard<std::mutex> lock(flushMutex_);
            pendingFlush_ = true;
        }
        flushCV_.notify_one();
        return Status::OK;
    } catch (const std::exception& e) {
        std::cerr << "Warning: Failed to write to WAL: " << e.what() << "\n";
        return Status::ERROR;
    }
}

Status WAL::logDel(const std::string& key) {
    if (!enabled || !logFile.is_open()) {
        return Status::ERROR;
    }
    
    try {
        std::string content = "DEL " + key;
        uint32_t crc = computeCRC32(content);
        logFile << content << " CRC:" << std::hex << std::setw(8) << std::setfill('0')
            << crc << std::dec << std::setfill(' ') << "\n";
        logFile.flush(); // Ensure it's written to disk
        // Group commit: signal background thread to fsync within 5ms
        {
            std::lock_guard<std::mutex> lock(flushMutex_);
            pendingFlush_ = true;
        }
        flushCV_.notify_one();
        return Status::OK;
    } catch (const std::exception& e) {
        std::cerr << "Warning: Failed to write to WAL: " << e.what() << "\n";
        return Status::ERROR;
    }
}

Status WAL::logPolicy(const std::string& policyName) {
    if (!enabled || !logFile.is_open()) {
        return Status::ERROR;
    }
    
    try {
        std::string content = "POLICY SET " + policyName;
        uint32_t crc = computeCRC32(content);
        logFile << content << " CRC:" << std::hex << std::setw(8) << std::setfill('0')
            << crc << std::dec << std::setfill(' ') << "\n";
        logFile.flush(); // Ensure it's written to disk
        // Group commit: signal background thread to fsync within 5ms
        {
            std::lock_guard<std::mutex> lock(flushMutex_);
            pendingFlush_ = true;
        }
        flushCV_.notify_one();
        return Status::OK;
    } catch (const std::exception& e) {
        std::cerr << "Warning: Failed to write to WAL: " << e.what() << "\n";
        return Status::ERROR;
    }
}

Status WAL::logGuardAdd(const std::string& guardType, const std::string& guardName,
                        const std::string& keyPattern, const std::string& params) {
    if (!enabled || !logFile.is_open()) {
        return Status::ERROR;
    }
    
    try {
        std::string content = "GUARD ADD " + guardType + " " + guardName + " "
            + keyPattern + " " + params;
        uint32_t crc = computeCRC32(content);
        logFile << content << " CRC:" << std::hex << std::setw(8) << std::setfill('0')
                << crc << std::dec << std::setfill(' ') << "\n";
        logFile.flush();
        // Group commit: signal background thread to fsync within 5ms
        {
            std::lock_guard<std::mutex> lock(flushMutex_);
            pendingFlush_ = true;
        }
        flushCV_.notify_one();
        return Status::OK;
    } catch (const std::exception& e) {
        std::cerr << "Warning: Failed to write guard to WAL: " << e.what() << "\n";
        return Status::ERROR;
    }
}

std::vector<std::string> WAL::readLog() {
    std::vector<std::string> commands;
    size_t checksumErrors = 0;
    
    try {
        if (!fileExists(walPath)) {
            std::cout << "No existing WAL file found (starting fresh)\n";
            return commands;
        }
        
        std::ifstream inFile(walPath);
        if (!inFile.is_open()) {
            std::cerr << "Warning: Failed to open WAL for reading: " << walPath << "\n";
            return commands;
        }
        
        std::string line;
        const std::string crcMarker = " CRC:";
        while (std::getline(inFile, line)) {
            if (!line.empty()) {
                size_t markerPos = line.rfind(crcMarker);
                if (markerPos != std::string::npos && markerPos + crcMarker.size() < line.size()) {
                    std::string content = line.substr(0, markerPos);
                    std::string crcHex = line.substr(markerPos + crcMarker.size());

                    bool isHex = (crcHex.size() == 8);
                    for (char c : crcHex) {
                        if (!std::isxdigit(static_cast<unsigned char>(c))) {
                            isHex = false;
                            break;
                        }
                    }

                    if (isHex) {
                        try {
                            uint32_t expectedCrc = static_cast<uint32_t>(std::stoul(crcHex, nullptr, 16));
                            uint32_t actualCrc = computeCRC32(content);
                            if (actualCrc != expectedCrc) {
                                std::cerr << "Warning: WAL checksum mismatch, skipping corrupt record\n";
                                checksumErrors++;
                                continue;
                            }
                            commands.push_back(content);
                            continue;
                        } catch (...) {
                            // Fall through to legacy handling below
                        }
                    }
                }

                // Legacy record without CRC field (backward compatible)
                commands.push_back(line);
            }
        }
        
        inFile.close();

        if (checksumErrors > 0) {
            std::cerr << "WARNING: " << checksumErrors
                      << " WAL records had checksum errors and were skipped (possible corruption)\n";
        }
        
        if (!commands.empty()) {
            std::cout << "Loaded " << commands.size() << " commands from WAL\n";
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Warning: Error reading WAL: " << e.what() << "\n";
    }
    
    return commands;
}

std::vector<std::string> WAL::readSnapshot() {
    std::vector<std::string> commands;
    
    try {
        if (!fileExists(snapshotPath)) {
            // No snapshot exists, which is fine
            return commands;
        }
        
        std::ifstream inFile(snapshotPath);
        if (!inFile.is_open()) {
            std::cerr << "Warning: Failed to open snapshot for reading: " << snapshotPath << "\n";
            return commands;
        }
        
        std::string line;
        while (std::getline(inFile, line)) {
            if (!line.empty()) {
                commands.push_back(line);
            }
        }
        
        inFile.close();
        
        if (!commands.empty()) {
            std::cout << "Loaded snapshot with " << commands.size() << " keys\n";
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Warning: Error reading snapshot: " << e.what() << "\n";
    }
    
    return commands;
}

Status WAL::createSnapshot(const std::unordered_map<std::string, std::string>& data,
                          const std::string& currentPolicy,
                          const std::vector<std::string>& guardCommands,
                          const std::string& retentionCommand) {
    try {
        // Open snapshot file for writing (truncate mode)
        std::ofstream snapFile(snapshotPath, std::ios::trunc);
        if (!snapFile.is_open()) {
            std::cerr << "Error: Failed to create snapshot file: " << snapshotPath << "\n";
            return Status::ERROR;
        }
        
        // Write policy first (if provided)
        if (!currentPolicy.empty()) {
            snapFile << "POLICY SET " << currentPolicy << "\n";
        }
        
        // Write retention configuration (if provided)
        if (!retentionCommand.empty()) {
            snapFile << retentionCommand << "\n";
        }

        // Write guards
        for (const auto& guardLine : guardCommands) {
            if (!guardLine.empty()) {
                snapFile << guardLine << "\n";
            }
        }

        // Write all key-value pairs as SET commands.
        // Keys and values are quoted so spaces and escaped characters survive replay.
        for (const auto& [key, value] : data) {
            snapFile << "SET " << std::quoted(key) << " " << std::quoted(value) << "\n";
        }
        
        snapFile.flush();
        int snapFd = ::open(snapshotPath.c_str(), O_WRONLY, 0644);
        // fsync: force kernel buffer to physical disk
        if (snapFd != -1) { ::fsync(snapFd); ::close(snapFd); }
        snapFile.close();
        
        // Clear the WAL after successful snapshot
        Status clearStatus = clearLog();
        if (clearStatus != Status::OK) {
            std::cerr << "Warning: Snapshot created but failed to clear WAL\n";
            return Status::ERROR;
        }
        
        std::cout << "Snapshot created with " << data.size() << " keys\n";
        return Status::OK;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: Failed to create snapshot: " << e.what() << "\n";
        return Status::ERROR;
    }
}

bool WAL::isEnabled() const {
    return enabled;
}

void WAL::flush() {
    if (logFile.is_open()) {
        try {
            logFile.flush();
        } catch (...) {
            // Silently handle flush errors
        }
    }
}

bool WAL::createDirectory(const std::string& path) {
    try {
        struct stat st;
        
        // Check if directory already exists
        if (stat(path.c_str(), &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                return true; // Directory exists
            }
            return false; // Path exists but is not a directory
        }
        
        // Create directory with permissions 0755
        if (mkdir(path.c_str(), 0755) == 0) {
            return true;
        }
        
        // Check if it failed because directory now exists (race condition)
        if (errno == EEXIST) {
            return true;
        }
        
        std::cerr << "Failed to create directory: " << strerror(errno) << "\n";
        return false;
        
    } catch (...) {
        return false;
    }
}

bool WAL::fileExists(const std::string& path) {
    struct stat st;
    return (stat(path.c_str(), &st) == 0);
}

Status WAL::clearLog() {
    try {
        // Close current log file
        if (logFile.is_open()) {
            logFile.close();
        }
        
        // Open in truncate mode to clear contents
        std::ofstream clearFile(walPath, std::ios::trunc);
        if (!clearFile.is_open()) {
            std::cerr << "Error: Failed to clear WAL log\n";
            enabled = false;
            return Status::ERROR;
        }
        clearFile.close();
        
        // Reopen in append mode
        logFile.open(walPath, std::ios::app);
        if (!logFile.is_open()) {
            std::cerr << "Error: Failed to reopen WAL after clearing\n";
            enabled = false;
            return Status::ERROR;
        }
        if (logFd_ != -1) { ::close(logFd_); }
        logFd_ = ::open(walPath.c_str(), O_WRONLY | O_APPEND, 0644);
        
        std::cout << "WAL log cleared\n";
        return Status::OK;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: Failed to clear WAL: " << e.what() << "\n";
        enabled = false;
        return Status::ERROR;
    }
}
