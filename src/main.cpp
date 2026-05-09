#include <iostream>
#include <string>
#include <memory>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <algorithm>
#include <cctype>
#include <optional>
#include "kvstore.h"
#include "command_parser.h"
#include "command.h"
#include "status.h"
#include "wal.h"

namespace {
    bool isAllDigits(const std::string& text) {
        if (text.empty()) {
            return false;
        }
        return std::all_of(text.begin(), text.end(), [](unsigned char c) {
            return std::isdigit(c);
        });
    }

    std::string joinTokens(const std::vector<std::string>& tokens) {
        std::ostringstream oss;
        for (size_t i = 0; i < tokens.size(); ++i) {
            if (i > 0) {
                oss << ' ';
            }
            oss << tokens[i];
        }
        return oss.str();
    }

    bool parseSetLine(const std::string& cmdLine,
                      std::string& key,
                      std::string& value,
                      std::optional<long long>& timestampMs,
                      bool allowTimestamp) {
        std::istringstream iss(cmdLine);
        std::string cmdType;
        if (!(iss >> cmdType) || cmdType != "SET") {
            return false;
        }

        if (!(iss >> std::quoted(key))) {
            return false;
        }

        std::string remainder;
        std::getline(iss >> std::ws, remainder);
        if (remainder.empty()) {
            return false;
        }

        if (!remainder.empty() && remainder.front() == '"') {
            std::istringstream valueStream(remainder);
            if (!(valueStream >> std::quoted(value))) {
                return false;
            }

            long long ts = 0;
            if (allowTimestamp && (valueStream >> ts)) {
                timestampMs = ts;
            }
            return true;
        }

        std::istringstream valueStream(remainder);
        std::vector<std::string> parts;
        std::string part;
        while (valueStream >> part) {
            parts.push_back(part);
        }
        if (parts.empty()) {
            return false;
        }

        if (allowTimestamp && parts.size() > 1 && isAllDigits(parts.back())) {
            try {
                timestampMs = std::stoll(parts.back());
                parts.pop_back();
            } catch (...) {
                timestampMs.reset();
            }
        }

        value = joinTokens(parts);
        return true;
    }
}

class RedisLikeCLI {
private:
    std::shared_ptr<KVStore> kvstore;
    std::shared_ptr<WAL> wal;
    bool running;

public:
    explicit RedisLikeCLI(std::shared_ptr<KVStore> store, std::shared_ptr<WAL> walPtr) 
        : kvstore(store), wal(walPtr), running(true) {}

    void run() {
        std::cout << "Redis-like Key-Value Database\n";
        std::cout << "Commands: SET key value | GET key | GET key AT <timestamp> | HISTORY key\n";
        std::cout << "          DEL key | SNAPSHOT | CONFIG RETENTION <mode> | EXIT\n";
        std::cout << "Type 'EXIT' to quit\n\n";

        while (running) {
            std::cout << "redis> ";
            std::string input;
            
            // Read entire line
            if (!std::getline(std::cin, input)) {
                // EOF reached (Ctrl+D)
                std::cout << "\nExiting...\n";
                break;
            }

            // Handle empty input
            if (input.empty()) {
                continue;
            }

            // Parse command
            Command cmd = CommandParser::parse(input);
            
            // Execute command
            executeCommand(cmd);
        }
    }

private:
    void executeCommand(const Command& cmd) {
        try {
            switch (cmd.type) {
                case CommandType::SET:
                    handleSet(cmd);
                    break;
                
                case CommandType::GET:
                    handleGet(cmd);
                    break;
                
                case CommandType::GETAT:
                    handleGetAt(cmd);
                    break;
                
                case CommandType::HISTORY:
                    handleHistory(cmd);
                    break;
                
                case CommandType::EXPLAIN:
                    handleExplain(cmd);
                    break;
                
                case CommandType::DEL:
                    handleDel(cmd);
                    break;
                
                case CommandType::SNAPSHOT:
                    handleSnapshot();
                    break;
                
                case CommandType::CONFIG:
                    handleConfig(cmd);
                    break;
                
                case CommandType::PROPOSE:
                    handlePropose(cmd);
                    break;
                
                case CommandType::GUARD:
                    handleGuard(cmd);
                    break;
                
                case CommandType::POLICY:
                    handlePolicy(cmd);
                    break;
                
                case CommandType::EXIT:
                    handleExit();
                    break;
                
                case CommandType::INVALID:
                    std::cout << "(error) ERR unknown command\n";
                    break;
            }
        } catch (const std::exception& e) {
            std::cout << "(error) ERR " << e.what() << "\n";
        } catch (...) {
            std::cout << "(error) ERR unknown error occurred\n";
        }
    }

    void handleSet(const Command& cmd) {
        if (cmd.args.size() < 2) {
            std::cout << "(error) ERR wrong number of arguments for 'SET' command\n";
            return;
        }

        const std::string& key = cmd.args[0];
        const std::string& value = cmd.args[1];
        
        Status status = kvstore->set(key, value);
        if (status == Status::OK) {
            std::cout << "OK\n";
        } else {
            std::cout << "(error) ERR failed to set key\n";
        }
    }

    void handleGet(const Command& cmd) {
        if (cmd.args.empty()) {
            std::cout << "(error) ERR wrong number of arguments for 'GET' command\n";
            return;
        }

        const std::string& key = cmd.args[0];
        auto result = kvstore->get(key);
        
        if (result.has_value()) {
            std::cout << "\"" << result.value() << "\"\n";
        } else {
            std::cout << "(nil)\n";
        }
    }

    void handleDel(const Command& cmd) {
        if (cmd.args.empty()) {
            std::cout << "(error) ERR wrong number of arguments for 'DEL' command\n";
            return;
        }

        const std::string& key = cmd.args[0];
        Status status = kvstore->del(key);
        
        if (status == Status::OK) {
            std::cout << "(integer) 1\n";
        } else {
            std::cout << "(integer) 0\n";
        }
    }

    std::string buildRetentionSnapshotCommand() const {
        const RetentionPolicy& policy = kvstore->getRetentionPolicy();
        switch (policy.mode) {
            case RetentionMode::FULL:
                return "CONFIG RETENTION FULL";
            case RetentionMode::LAST_N:
                return "CONFIG RETENTION LAST " + std::to_string(policy.count);
            case RetentionMode::LAST_T:
                return "CONFIG RETENTION LAST " + std::to_string(policy.seconds) + "s";
        }
        return "";
    }

    std::string joinGuardValues(const std::vector<std::string>& values) const {
        std::ostringstream oss;
        for (size_t i = 0; i < values.size(); ++i) {
            if (i > 0) {
                oss << ' ';
            }
            oss << values[i];
        }
        return oss.str();
    }

    std::vector<std::string> buildGuardSnapshotCommands() const {
        std::vector<std::string> commands;
        const auto& guards = kvstore->getGuards();

        for (const auto& guard : guards) {
            if (auto rangeGuard = std::dynamic_pointer_cast<RangeIntGuard>(guard)) {
                std::ostringstream oss;
                oss << "GUARD ADD RANGE_INT " << rangeGuard->getName() << " "
                    << rangeGuard->getKeyPattern() << " "
                    << rangeGuard->getMinValue() << " " << rangeGuard->getMaxValue();
                commands.push_back(oss.str());
            } else if (auto enumGuard = std::dynamic_pointer_cast<EnumGuard>(guard)) {
                const auto& values = enumGuard->getAllowedValues();
                if (!values.empty()) {
                    std::ostringstream oss;
                    oss << "GUARD ADD ENUM " << enumGuard->getName() << " "
                        << enumGuard->getKeyPattern() << " "
                        << joinGuardValues(values);
                    commands.push_back(oss.str());
                }
            } else if (auto lengthGuard = std::dynamic_pointer_cast<LengthGuard>(guard)) {
                std::ostringstream oss;
                oss << "GUARD ADD LENGTH " << lengthGuard->getName() << " "
                    << lengthGuard->getKeyPattern() << " "
                    << lengthGuard->getMinLength() << " " << lengthGuard->getMaxLength();
                commands.push_back(oss.str());
            }
        }

        return commands;
    }
    
    void handleSnapshot() {
        if (!wal || !wal->isEnabled()) {
            std::cout << "(error) ERR WAL not available\n";
            return;
        }
        
        // Get current policy name
        DecisionPolicy policy = kvstore->getDecisionPolicy();
        std::string policyName;
        switch (policy) {
            case DecisionPolicy::DEV_FRIENDLY:
                policyName = "DEV_FRIENDLY";
                break;
            case DecisionPolicy::SAFE_DEFAULT:
                policyName = "SAFE_DEFAULT";
                break;
            case DecisionPolicy::STRICT:
                policyName = "STRICT";
                break;
        }
        
        std::vector<std::string> guardCommands = buildGuardSnapshotCommands();
        std::string retentionCommand = buildRetentionSnapshotCommand();

        // Create snapshot with current store data, policy, guards, and retention config
        Status status = wal->createSnapshot(kvstore->getAllData(), policyName, guardCommands, retentionCommand);
        
        if (status == Status::OK) {
            std::cout << "OK\n";
        } else {
            std::cout << "(error) ERR failed to create snapshot\n";
        }
    }
    
    // Parse timestamp from string (supports epoch milliseconds or ISO-8601)
    std::optional<std::chrono::system_clock::time_point> parseTimestamp(const std::string& timestampStr) {
        // Try parsing as epoch milliseconds
        try {
            size_t pos;
            long long epochMs = std::stoll(timestampStr, &pos);
            // Ensure the entire string was consumed (it's a pure number)
            if (pos == timestampStr.length()) {
                return std::chrono::system_clock::time_point(std::chrono::milliseconds(epochMs));
            }
        } catch (...) {
            // Not a number, continue to ISO-8601 parsing
        }
        
        // Try parsing ISO-8601 format: YYYY-MM-DD HH:MM:SS.mmm or YYYY-MM-DD HH:MM:SS
        std::tm tm = {};
        std::istringstream ss(timestampStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        
        if (!ss.fail()) {
            // mktime uses local timezone, which matches formatTimestamp behavior
            tm.tm_isdst = -1;  // Let mktime determine DST
            auto timeT = std::mktime(&tm);
            
            if (timeT != -1) {
                auto result = std::chrono::system_clock::from_time_t(timeT);
                
                // Try to parse milliseconds if present (format: .mmm)
                char dot;
                int milliseconds;
                if (ss >> dot >> milliseconds) {
                    if (dot == '.') {
                        result += std::chrono::milliseconds(milliseconds);
                    }
                }
                
                return result;
            }
        }
        
        return std::nullopt;
    }
    
    // Format timestamp as human-readable string
    std::string formatTimestamp(const std::chrono::system_clock::time_point& timestamp) {
        auto timeT = std::chrono::system_clock::to_time_t(timestamp);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            timestamp.time_since_epoch()) % 1000;
        
        std::tm tm = *std::localtime(&timeT);
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        oss << "." << std::setfill('0') << std::setw(3) << ms.count();
        
        return oss.str();
    }
    
    void handleGetAt(const Command& cmd) {
        if (cmd.args.size() < 2) {
            std::cout << "(error) ERR wrong number of arguments for 'GET AT' command\n";
            return;
        }

        const std::string& key = cmd.args[0];
        const std::string& timestampStr = cmd.args[1];
        
        // Parse timestamp
        auto timestamp = parseTimestamp(timestampStr);
        if (!timestamp.has_value()) {
            std::cout << "(error) ERR invalid timestamp format. Use epoch milliseconds or 'YYYY-MM-DD HH:MM:SS'\n";
            return;
        }
        
        // Query historical value
        auto result = kvstore->getAtTime(key, timestamp.value());
        
        if (result.has_value()) {
            std::cout << "\"" << result.value() << "\"\n";
        } else {
            std::cout << "(nil)\n";
        }
    }
    
    void handleHistory(const Command& cmd) {
        if (cmd.args.empty()) {
            std::cout << "(error) ERR wrong number of arguments for 'HISTORY' command\n";
            return;
        }

        const std::string& key = cmd.args[0];
        auto history = kvstore->getHistory(key);
        
        if (history.empty()) {
            std::cout << "(empty array)\n";
            return;
        }
        
        // Display history with timestamps
        std::cout << history.size() << " version(s):\n";
        int idx = 1;
        for (const auto& version : history) {
            std::cout << idx++ << ") [" << formatTimestamp(version.timestamp) 
                      << "] \"" << version.value << "\"\n";
        }
    }
    
    void handleExplain(const Command& cmd) {
        if (cmd.args.size() < 2) {
            std::cout << "(error) ERR wrong number of arguments for 'EXPLAIN' command\n";
            std::cout << "Usage: EXPLAIN GET <key> AT <timestamp>\n";
            return;
        }

        const std::string& key = cmd.args[0];
        const std::string& timestampStr = cmd.args[1];
        
        // Parse timestamp
        auto timestamp = parseTimestamp(timestampStr);
        if (!timestamp.has_value()) {
            std::cout << "(error) ERR invalid timestamp format. Use epoch milliseconds or 'YYYY-MM-DD HH:MM:SS'\n";
            return;
        }
        
        // Get explanation
        auto result = kvstore->explainGetAtTime(key, timestamp.value());
        
        // Display structured output
        std::cout << "\n========== EXPLAIN GET AT ==========\n";
        std::cout << "Query:     GET \"" << result.key << "\" AT " << formatTimestamp(result.queryTimestamp) << "\n";
        std::cout << "Status:    " << (result.found ? "FOUND" : "NOT FOUND") << "\n";
        std::cout << "Total Versions: " << result.totalVersions << "\n";
        std::cout << "\n";
        
        if (result.found && result.selectedVersion.has_value()) {
            const auto& selected = result.selectedVersion.value();
            std::cout << "Selected Version:\n";
            std::cout << "  Timestamp: " << formatTimestamp(selected.timestamp) << "\n";
            std::cout << "  Value:     \"" << selected.value << "\"\n";
            std::cout << "\n";
        }
        
        std::cout << "Reasoning:\n  " << result.reasoning << "\n";
        
        if (!result.skippedVersions.empty()) {
            std::cout << "\nSkipped Versions (superseded by selected version):\n";
            int idx = 1;
            for (const auto& version : result.skippedVersions) {
                std::cout << "  " << idx++ << ") [" << formatTimestamp(version.timestamp)
                          << "] \"" << version.value << "\"\n";
            }
        }
        
        std::cout << "====================================\n\n";
    }
    
    void handleConfig(const Command& cmd) {
        if (cmd.args.size() < 2) {
            std::cout << "(error) ERR wrong number of arguments for 'CONFIG' command\n";
            std::cout << "Usage: CONFIG RETENTION FULL | CONFIG RETENTION LAST <N> | CONFIG RETENTION LAST <T>s\n";
            return;
        }
        
        // Convert subcommand to uppercase
        std::string subcommand = cmd.args[0];
        std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        
        if (subcommand != "RETENTION") {
            std::cout << "(error) ERR unknown CONFIG subcommand '" << cmd.args[0] << "'\n";
            return;
        }
        
        // Parse retention mode
        std::string modeStr = cmd.args[1];
        std::transform(modeStr.begin(), modeStr.end(), modeStr.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        
        RetentionPolicy policy;
        
        if (modeStr == "FULL") {
            policy.mode = RetentionMode::FULL;
            std::cout << "OK - Retention policy set to FULL (keep all versions)\n";
        } else if (modeStr == "LAST") {
            // Parse: LAST <N> or LAST <T>s
            if (cmd.args.size() < 3) {
                std::cout << "(error) ERR LAST requires a value parameter\n";
                std::cout << "Usage: CONFIG RETENTION LAST <N> for count, or CONFIG RETENTION LAST <T>s for time\n";
                return;
            }
            
            std::string valueStr = cmd.args[2];
            
            // Check if value ends with 's' (time-based retention)
            if (!valueStr.empty() && valueStr.back() == 's') {
                // LAST_T mode: strip 's' and parse as seconds
                std::string secondsStr = valueStr.substr(0, valueStr.length() - 1);
                if (secondsStr.empty()) {
                    std::cout << "(error) ERR invalid format, expected number before 's'\n";
                    return;
                }
                try {
                    size_t pos;
                    int seconds = std::stoi(secondsStr, &pos);
                    // Check if entire string was parsed
                    if (pos != secondsStr.length()) {
                        std::cout << "(error) ERR invalid seconds value\n";
                        return;
                    }
                    if (seconds <= 0) {
                        std::cout << "(error) ERR seconds must be positive\n";
                        return;
                    }
                    policy = RetentionPolicy(RetentionMode::LAST_T, seconds);
                    std::cout << "OK - Retention policy set to LAST " << seconds << "s (keep versions from last " << seconds << " seconds)\n";
                } catch (...) {
                    std::cout << "(error) ERR invalid seconds value\n";
                    return;
                }
            } else {
                // LAST_N mode: parse as count
                try {
                    size_t pos;
                    int count = std::stoi(valueStr, &pos);
                    // Check if entire string was parsed
                    if (pos != valueStr.length()) {
                        std::cout << "(error) ERR invalid count value\n";
                        return;
                    }
                    if (count <= 0) {
                        std::cout << "(error) ERR count must be positive\n";
                        return;
                    }
                    policy = RetentionPolicy(RetentionMode::LAST_N, count);
                    std::cout << "OK - Retention policy set to LAST " << count << " (keep last " << count << " versions)\n";
                } catch (...) {
                    std::cout << "(error) ERR invalid count value\n";
                    return;
                }
            }
        } else {
            std::cout << "(error) ERR unknown retention mode '" << cmd.args[1] << "'\n";
            std::cout << "Valid modes: FULL, LAST <N>, LAST <T>s\n";
            return;
        }
        
        // Apply the new retention policy
        kvstore->setRetentionPolicy(policy);
    }
    
    void handlePropose(const Command& cmd) {
        if (cmd.args.size() < 2) {
            std::cout << "(error) ERR wrong number of arguments for 'PROPOSE' command\n";
            std::cout << "Usage: PROPOSE SET key value\n";
            return;
        }
        
        // Check if first arg is SET
        std::string operation = cmd.args[0];
        std::transform(operation.begin(), operation.end(), operation.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        
        if (operation != "SET") {
            std::cout << "(error) ERR PROPOSE currently only supports SET operation\n";
            return;
        }
        
        if (cmd.args.size() < 3) {
            std::cout << "(error) ERR PROPOSE SET requires key and value\n";
            return;
        }
        
        const std::string& key = cmd.args[1];
        const std::string& value = cmd.args[2];
        
        // Evaluate the proposed write
        auto evaluation = kvstore->proposeSet(key, value);
        
        std::cout << "\n========== WRITE EVALUATION ==========\n";
        std::cout << "Proposal:  SET \"" << key << "\" \"" << value << "\"\n";
        
        switch (evaluation.result) {
            case GuardResult::ACCEPT:
                std::cout << "Result:    ACCEPT ✓\n";
                std::cout << "Reason:    " << evaluation.reason << "\n";
                std::cout << "\nThe write is safe to commit. Use: SET " << key << " " << value << "\n";
                break;
                
            case GuardResult::REJECT:
                std::cout << "Result:    REJECT ✗\n";
                std::cout << "Reason:    " << evaluation.reason << "\n";
                if (!evaluation.triggeredGuards.empty()) {
                    std::cout << "Triggered: ";
                    for (size_t i = 0; i < evaluation.triggeredGuards.size(); ++i) {
                        if (i > 0) std::cout << ", ";
                        std::cout << evaluation.triggeredGuards[i];
                    }
                    std::cout << "\n";
                }
                std::cout << "\nThis write cannot be performed.\n";
                break;
                
            case GuardResult::COUNTER_OFFER:
                std::cout << "Result:    COUNTER_OFFER ⚠\n";
                std::cout << "Reason:    " << evaluation.reason << "\n";
                if (!evaluation.triggeredGuards.empty()) {
                    std::cout << "Triggered: ";
                    for (size_t i = 0; i < evaluation.triggeredGuards.size(); ++i) {
                        if (i > 0) std::cout << ", ";
                        std::cout << evaluation.triggeredGuards[i];
                    }
                    std::cout << "\n";
                }
                
                if (!evaluation.alternatives.empty()) {
                    std::cout << "\nSafe Alternatives:\n";
                    for (size_t i = 0; i < evaluation.alternatives.size(); ++i) {
                        const auto& alt = evaluation.alternatives[i];
                        std::cout << "  " << (i + 1) << ") \"" << alt.value << "\"\n";
                        std::cout << "     → " << alt.reason << "\n";
                        std::cout << "       confidence=" << std::fixed << std::setprecision(2)
                                  << alt.confidence << " risk=" << riskLevelToString(alt.risk) << "\n";
                    }
                }
                break;
        }
        
        std::cout << "======================================\n\n";
    }
    
    void handleGuard(const Command& cmd) {
        if (cmd.args.empty()) {
            std::cout << "(error) ERR wrong number of arguments for 'GUARD' command\n";
            std::cout << "Usage:\n";
            std::cout << "  GUARD ADD <type> <name> <key_pattern> <params...>\n";
            std::cout << "  GUARD LIST\n";
            std::cout << "  GUARD REMOVE <name>\n";
            return;
        }
        
        std::string subcommand = cmd.args[0];
        std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        
        if (subcommand == "LIST") {
            const auto& guards = kvstore->getGuards();
            if (guards.empty()) {
                std::cout << "No guards defined\n";
                return;
            }
            
            std::cout << guards.size() << " guard(s) defined:\n";
            for (size_t i = 0; i < guards.size(); ++i) {
                const auto& guard = guards[i];
                std::cout << (i + 1) << ") " << guard->getName() 
                          << " (key: " << guard->getKeyPattern() << ")\n";
                std::cout << "   " << guard->describe() << "\n";
                std::cout << "   Status: " << (guard->isEnabled() ? "enabled" : "disabled") << "\n";
            }
        } else if (subcommand == "ADD") {
            // GUARD ADD RANGE_INT name key min max
            // GUARD ADD ENUM name key val1,val2,val3
            // GUARD ADD LENGTH name key min max
            
            if (cmd.args.size() < 4) {
                std::cout << "(error) ERR insufficient arguments for GUARD ADD\n";
                return;
            }
            
            std::string type = cmd.args[1];
            std::transform(type.begin(), type.end(), type.begin(),
                           [](unsigned char c) { return std::toupper(c); });
            
            std::string name = cmd.args[2];
            std::string keyPattern = cmd.args[3];
            
            try {
                if (type == "RANGE_INT" || type == "RANGE") {
                    if (cmd.args.size() < 6) {
                        std::cout << "(error) ERR RANGE_INT requires: name key min max\n";
                        return;
                    }
                    int min = std::stoi(cmd.args[4]);
                    int max = std::stoi(cmd.args[5]);
                    
                    auto guard = std::make_shared<RangeIntGuard>(name, keyPattern, min, max);
                    kvstore->addGuard(guard);
                    std::cout << "OK - Added range guard '" << name << "' for key pattern '" 
                              << keyPattern << "': [" << min << ", " << max << "]\n";
                    
                } else if (type == "ENUM") {
                    if (cmd.args.size() < 5) {
                        std::cout << "(error) ERR ENUM requires: name key values\n";
                        return;
                    }
                    
                    // Parse comma-separated values
                    std::string valuesStr = cmd.args[4];
                    std::vector<std::string> values;
                    std::stringstream ss(valuesStr);
                    std::string value;
                    while (std::getline(ss, value, ',')) {
                        values.push_back(value);
                    }
                    
                    auto guard = std::make_shared<EnumGuard>(name, keyPattern, values);
                    kvstore->addGuard(guard);
                    std::cout << "OK - Added enum guard '" << name << "' for key pattern '" 
                              << keyPattern << "' with " << values.size() << " allowed values\n";
                    
                } else if (type == "LENGTH") {
                    if (cmd.args.size() < 6) {
                        std::cout << "(error) ERR LENGTH requires: name key min max\n";
                        return;
                    }
                    size_t min = std::stoul(cmd.args[4]);
                    size_t max = std::stoul(cmd.args[5]);
                    
                    auto guard = std::make_shared<LengthGuard>(name, keyPattern, min, max);
                    kvstore->addGuard(guard);
                    std::cout << "OK - Added length guard '" << name << "' for key pattern '" 
                              << keyPattern << "': [" << min << ", " << max << "] characters\n";
                    
                } else {
                    std::cout << "(error) ERR unknown guard type '" << type << "'\n";
                    std::cout << "Available types: RANGE_INT, ENUM, LENGTH\n";
                }
            } catch (const std::exception& e) {
                std::cout << "(error) ERR failed to create guard: " << e.what() << "\n";
            }
            
        } else if (subcommand == "REMOVE") {
            if (cmd.args.size() < 2) {
                std::cout << "(error) ERR GUARD REMOVE requires guard name\n";
                return;
            }
            
            std::string name = cmd.args[1];
            if (kvstore->removeGuard(name)) {
                std::cout << "OK - Removed guard '" << name << "'\n";
            } else {
                std::cout << "(error) ERR guard '" << name << "' not found\n";
            }
        } else {
            std::cout << "(error) ERR unknown GUARD subcommand '" << subcommand << "'\n";
            std::cout << "Available: ADD, LIST, REMOVE\n";
        }
    }

    void handlePolicy(const Command& cmd) {
        if (cmd.args.empty()) {
            std::cout << "(error) ERR wrong number of arguments for 'POLICY' command\n";
            std::cout << "Usage:\n";
            std::cout << "  POLICY GET - Display current decision policy\n";
            std::cout << "  POLICY SET <policy> - Set decision policy (DEV_FRIENDLY, SAFE_DEFAULT, STRICT)\n";
            return;
        }
        
        std::string subcommand = cmd.args[0];
        std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        
        if (subcommand == "GET") {
            DecisionPolicy policy = kvstore->getDecisionPolicy();
            std::string policyName;
            std::string policyDescription;
            
            switch (policy) {
                case DecisionPolicy::DEV_FRIENDLY:
                    policyName = "DEV_FRIENDLY";
                    policyDescription = "Always negotiate guard violations when alternatives exist";
                    break;
                case DecisionPolicy::SAFE_DEFAULT:
                    policyName = "SAFE_DEFAULT";
                    policyDescription = "Negotiate low-risk violations (with alternatives), reject high-risk (no alternatives)";
                    break;
                case DecisionPolicy::STRICT:
                    policyName = "STRICT";
                    policyDescription = "Reject all guard violations without negotiation";
                    break;
            }
            
            std::cout << "Current decision policy: " << policyName << "\n";
            std::cout << "Description: " << policyDescription << "\n";
            
        } else if (subcommand == "SET") {
            if (cmd.args.size() < 2) {
                std::cout << "(error) ERR POLICY SET requires policy name\n";
                std::cout << "Available policies: DEV_FRIENDLY, SAFE_DEFAULT, STRICT\n";
                return;
            }
            
            std::string policyStr = cmd.args[1];
            std::transform(policyStr.begin(), policyStr.end(), policyStr.begin(),
                           [](unsigned char c) { return std::toupper(c); });
            
            DecisionPolicy newPolicy;
            if (policyStr == "DEV_FRIENDLY") {
                newPolicy = DecisionPolicy::DEV_FRIENDLY;
            } else if (policyStr == "SAFE_DEFAULT") {
                newPolicy = DecisionPolicy::SAFE_DEFAULT;
            } else if (policyStr == "STRICT") {
                newPolicy = DecisionPolicy::STRICT;
            } else {
                std::cout << "(error) ERR unknown policy '" << policyStr << "'\n";
                std::cout << "Available policies: DEV_FRIENDLY, SAFE_DEFAULT, STRICT\n";
                return;
            }
            
            kvstore->setDecisionPolicy(newPolicy);
            std::cout << "OK - Decision policy set to " << policyStr << "\n";
            
        } else {
            std::cout << "(error) ERR unknown POLICY subcommand '" << subcommand << "'\n";
            std::cout << "Available: GET, SET\n";
        }
    }

    void handleExit() {
        std::cout << "Goodbye!\n";
        running = false;
    }
};

int main() {
    try {
        // Initialize WAL
        auto wal = std::make_shared<WAL>("data/wal.log");
        Status walStatus = wal->initialize();
        
        // Create KVStore with WAL
        auto kvstore = std::make_shared<KVStore>(wal);
        
        // Load snapshot first (if it exists)
        if (walStatus == Status::OK) {
            std::vector<std::string> snapshotCommands = wal->readSnapshot();
            if (!snapshotCommands.empty()) {
                std::cout << "Loading snapshot...\n";
                // Disable WAL during replay to avoid duplicate logging
                kvstore->setWalEnabled(false);
                auto snapshotTime = std::chrono::system_clock::now();
                
                // First pass: Load policy, retention config, and guards from snapshot
                for (const auto& cmdLine : snapshotCommands) {
                    std::istringstream iss(cmdLine);
                    std::string cmdType;
                    iss >> cmdType;

                    if (cmdType == "POLICY") {
                        std::string subCmd, policyName;
                        iss >> subCmd >> policyName;
                        if (subCmd == "SET") {
                            DecisionPolicy policy;
                            if (policyName == "DEV_FRIENDLY") {
                                policy = DecisionPolicy::DEV_FRIENDLY;
                            } else if (policyName == "SAFE_DEFAULT") {
                                policy = DecisionPolicy::SAFE_DEFAULT;
                            } else if (policyName == "STRICT") {
                                policy = DecisionPolicy::STRICT;
                            } else {
                                continue;
                            }
                            kvstore->setDecisionPolicy(policy);
                        }
                    } else if (cmdType == "CONFIG") {
                        std::string subCmd, modeToken;
                        iss >> subCmd >> modeToken;
                        std::transform(subCmd.begin(), subCmd.end(), subCmd.begin(), ::toupper);
                        std::transform(modeToken.begin(), modeToken.end(), modeToken.begin(), ::toupper);

                        if (subCmd == "RETENTION") {
                            if (modeToken == "FULL") {
                                kvstore->setRetentionPolicy(RetentionPolicy());
                            } else if (modeToken == "LAST") {
                                std::string valueToken;
                                iss >> valueToken;
                                if (!valueToken.empty()) {
                                    bool isTimeBased = false;
                                    if (!valueToken.empty() &&
                                        (valueToken.back() == 's' || valueToken.back() == 'S')) {
                                        isTimeBased = true;
                                        valueToken.pop_back();
                                    }

                                    try {
                                        int value = std::stoi(valueToken);
                                        if (value > 0) {
                                            if (isTimeBased) {
                                                kvstore->setRetentionPolicy(RetentionPolicy(RetentionMode::LAST_T, value));
                                            } else {
                                                kvstore->setRetentionPolicy(RetentionPolicy(RetentionMode::LAST_N, value));
                                            }
                                        }
                                    } catch (...) {
                                        // Ignore invalid retention config
                                    }
                                }
                            }
                        }
                    } else if (cmdType == "GUARD") {
                        std::string subCmd, guardType, name, keyPattern;
                        iss >> subCmd >> guardType >> name >> keyPattern;

                        if (subCmd == "ADD") {
                            try {
                                std::shared_ptr<Guard> guard;

                                if (guardType == "RANGE_INT") {
                                    int min, max;
                                    iss >> min >> max;
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<RangeIntGuard>(name, keyPattern, min, max);
                                        kvstore->addGuard(guard);
                                    }
                                } else if (guardType == "ENUM") {
                                    std::vector<std::string> values;
                                    std::string value;
                                    while (iss >> value) {
                                        values.push_back(value);
                                    }
                                    if (!values.empty() && !kvstore->hasGuard(name)) {
                                        guard = std::make_shared<EnumGuard>(name, keyPattern, values);
                                        kvstore->addGuard(guard);
                                    }
                                } else if (guardType == "LENGTH") {
                                    size_t min, max;
                                    iss >> min >> max;
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<LengthGuard>(name, keyPattern, min, max);
                                        kvstore->addGuard(guard);
                                    }
                                }
                            } catch (...) {
                                // Ignore invalid guard entries in snapshot
                            }
                        }
                    }
                }
                
                // Second pass: Load data from snapshot
                for (const auto& cmdLine : snapshotCommands) {
                    std::istringstream iss(cmdLine);
                    std::string cmdType;
                    iss >> cmdType;

                    // Only replay SET commands from snapshot.
                    // Use setAtTime to avoid logging, but use current time since snapshot doesn't store timestamps.
                    if (cmdType == "SET") {
                        std::string key, value;
                        std::optional<long long> ignoredTimestamp;
                        if (parseSetLine(cmdLine, key, value, ignoredTimestamp, false) && !key.empty()) {
                            kvstore->setAtTime(key, value, snapshotTime);
                        }
                    }
                }
                kvstore->setWalEnabled(true);
                std::cout << "Snapshot loaded. Restored " << kvstore->size() << " keys\n";
            }
            
            // Then replay WAL (changes since last snapshot)
            std::vector<std::string> commands = wal->readLog();
            if (!commands.empty()) {
                std::cout << "Replaying WAL...\n";
                // Disable WAL during replay to avoid duplicate logging
                kvstore->setWalEnabled(false);
                
                // PHASE 1: Replay POLICY commands first (before data)
                for (const auto& cmdLine : commands) {
                    std::istringstream iss(cmdLine);
                    std::string cmdType;
                    iss >> cmdType;
                    
                    if (cmdType == "POLICY") {
                        std::string subCmd, policyName;
                        iss >> subCmd >> policyName;
                        
                        if (subCmd == "SET") {
                            DecisionPolicy policy;
                            if (policyName == "DEV_FRIENDLY") {
                                policy = DecisionPolicy::DEV_FRIENDLY;
                            } else if (policyName == "SAFE_DEFAULT") {
                                policy = DecisionPolicy::SAFE_DEFAULT;
                            } else if (policyName == "STRICT") {
                                policy = DecisionPolicy::STRICT;
                            } else {
                                continue; // Skip invalid policy
                            }
                            
                            // Set policy without logging (we're replaying)
                            kvstore->setWalEnabled(false);
                            kvstore->setDecisionPolicy(policy);
                        }
                    }
                }
                
                // PHASE 2: Replay data commands
                for (const auto& cmdLine : commands) {
                    // Parse command line manually to extract timestamp
                    std::istringstream iss(cmdLine);
                    std::string cmdType;
                    iss >> cmdType;
                    
                    if (cmdType == "SET") {
                        std::string key, value;
                        std::optional<long long> timestampMs;
                        if (!parseSetLine(cmdLine, key, value, timestampMs, true) || key.empty()) {
                            continue;
                        }

                        if (timestampMs.has_value()) {
                            auto timestamp = std::chrono::system_clock::time_point(
                                std::chrono::milliseconds(timestampMs.value()));
                            kvstore->setAtTime(key, value, timestamp);
                        } else {
                            kvstore->setAtTime(key, value, std::chrono::system_clock::now());
                        }
                    } else if (cmdType == "DEL") {
                        std::string key;
                        iss >> key;
                        if (!key.empty()) {
                            kvstore->del(key);
                        }
                    }
                }
                kvstore->setWalEnabled(true);
                std::cout << "WAL replay complete. Restored " << kvstore->size() << " keys\n";
            }
        }
        
        std::cout << "\n";
        
        // Start CLI
        RedisLikeCLI cli(kvstore, wal);
        cli.run();
        
        // Flush WAL before exiting
        if (wal->isEnabled()) {
            wal->flush();
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "Fatal error: unknown exception\n";
        return 1;
    }
}
