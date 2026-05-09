#include <iostream>
#include <memory>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <atomic>
#include <csignal>
#include <thread>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include "../include/json.hpp"
#include "../include/external/httplib.h"
#include "../include/kvstore.h"
#include "../include/wal.h"
#include "../include/guard.h"
#include "../include/logger.h"
#include "../include/metrics.h"

using json = nlohmann::json;

// Global atomic flag for shutdown signal handling
std::atomic<bool> shutdownRequested{false};

// Signal handler for SIGINT and SIGTERM
void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        shutdownRequested.store(true);
    }
}

json parseRequestJSON(const std::string& body) {
    auto parsed = json::parse(body);
    if (!parsed.is_object()) {
        throw std::invalid_argument("JSON body must be an object");
    }
    return parsed;
}

std::string requireStringField(const json& body, const char* fieldName) {
    auto it = body.find(fieldName);
    if (it == body.end()) {
        throw std::invalid_argument(std::string("Missing '") + fieldName + "' parameter");
    }
    if (!it->is_string()) {
        throw std::invalid_argument(std::string("'") + fieldName + "' must be a string");
    }
    return it->get<std::string>();
}

std::string jsonValueToStoredString(const json& value) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    return value.dump();
}

int requireIntField(const json& body, const char* fieldName) {
    auto it = body.find(fieldName);
    if (it == body.end()) {
        throw std::invalid_argument(std::string("Missing '") + fieldName + "' parameter");
    }
    if (it->is_number_integer()) {
        return it->get<int>();
    }
    if (it->is_number_unsigned()) {
        return static_cast<int>(it->get<unsigned int>());
    }
    if (it->is_string()) {
        return std::stoi(it->get<std::string>());
    }
    throw std::invalid_argument(std::string("'") + fieldName + "' must be an integer");
}

double requireDoubleField(const json& body, const char* fieldName) {
    auto it = body.find(fieldName);
    if (it == body.end()) {
        throw std::invalid_argument(std::string("Missing '") + fieldName + "' parameter");
    }
    if (it->is_number()) {
        return it->get<double>();
    }
    if (it->is_string()) {
        return std::stod(it->get<std::string>());
    }
    throw std::invalid_argument(std::string("'") + fieldName + "' must be a number");
}

size_t requireSizeField(const json& body, const char* fieldName) {
    auto it = body.find(fieldName);
    if (it == body.end()) {
        throw std::invalid_argument(std::string("Missing '") + fieldName + "' parameter");
    }
    if (it->is_number_unsigned()) {
        return it->get<size_t>();
    }
    if (it->is_number_integer()) {
        int value = it->get<int>();
        if (value < 0) {
            throw std::invalid_argument(std::string("'") + fieldName + "' must be non-negative");
        }
        return static_cast<size_t>(value);
    }
    if (it->is_string()) {
        return std::stoul(it->get<std::string>());
    }
    throw std::invalid_argument(std::string("'") + fieldName + "' must be an integer");
}

std::vector<std::string> parseEnumValues(const json& body) {
    auto it = body.find("values");
    if (it == body.end()) {
        throw std::invalid_argument("Missing 'values' parameter");
    }

    std::vector<std::string> values;
    if (it->is_array()) {
        for (const auto& entry : *it) {
            values.push_back(jsonValueToStoredString(entry));
        }
    } else {
        std::stringstream ss(jsonValueToStoredString(*it));
        std::string value;
        while (std::getline(ss, value, ',')) {
            value.erase(0, value.find_first_not_of(" \t\n\r"));
            value.erase(value.find_last_not_of(" \t\n\r") + 1);
            if (!value.empty()) {
                values.push_back(value);
            }
        }
    }

    return values;
}

std::string joinWalValues(const std::vector<std::string>& values) {
    std::ostringstream oss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            oss << ' ';
        }
        oss << values[i];
    }
    return oss.str();
}

std::string escapeJSON(const std::string& str);

std::string formatConfidence(double value) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3) << value;
    return oss.str();
}

std::string guardResultToString(GuardResult result) {
    switch (result) {
        case GuardResult::ACCEPT: return "ACCEPT";
        case GuardResult::REJECT: return "REJECT";
        case GuardResult::COUNTER_OFFER: return "COUNTER_OFFER";
    }
    return "REJECT";
}

std::string decisionPolicyToString(DecisionPolicy policy) {
    switch (policy) {
        case DecisionPolicy::DEV_FRIENDLY: return "DEV_FRIENDLY";
        case DecisionPolicy::SAFE_DEFAULT: return "SAFE_DEFAULT";
        case DecisionPolicy::STRICT: return "STRICT";
    }
    return "SAFE_DEFAULT";
}

void appendWriteEvaluationJSON(std::stringstream& json, const std::string& key,
                               const std::string& value, const WriteEvaluation& evaluation) {
    json << "\"proposal\":{\"key\":\"" << escapeJSON(key)
         << "\",\"value\":\"" << escapeJSON(value) << "\"},";
    json << "\"result\":\"" << guardResultToString(evaluation.result) << "\",";
    json << "\"reason\":\"" << escapeJSON(evaluation.reason) << "\",";

    json << "\"triggeredGuards\":[";
    for (size_t i = 0; i < evaluation.triggeredGuards.size(); ++i) {
        if (i > 0) json << ",";
        json << "\"" << escapeJSON(evaluation.triggeredGuards[i]) << "\"";
    }
    json << "],";

    json << "\"alternatives\":[";
    for (size_t i = 0; i < evaluation.alternatives.size(); ++i) {
        if (i > 0) json << ",";
        const auto& alt = evaluation.alternatives[i];
        json << "{\"value\":\"" << escapeJSON(alt.value)
             << "\",\"confidence\":" << formatConfidence(alt.confidence)
             << ",\"risk\":\"" << escapeJSON(riskLevelToString(alt.risk))
             << "\",\"reason\":\"" << escapeJSON(alt.reason) << "\"}";
    }
    json << "]";
}

// Helper function to escape JSON strings
std::string escapeJSON(const std::string& str) {
    std::string result;
    for (char c : str) {
        switch (c) {
            case '"': result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\b': result += "\\b"; break;
            case '\f': result += "\\f"; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            default: result += c; break;
        }
    }
    return result;
}

// Helper function to format timestamp as ISO 8601
std::string formatTimestamp(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

// Helper function to parse ISO 8601 timestamp
std::chrono::system_clock::time_point parseTimestamp(const std::string& timeStr) {
    std::tm tm = {};
    std::istringstream ss(timeStr);
    
    // Parse date and time
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    
    // Parse milliseconds if present
    if (ss.peek() == '.') {
        ss.ignore();
        int ms;
        ss >> ms;
        tp += std::chrono::milliseconds(ms);
    }
    
    return tp;
}

int main(int argc, char* argv[]) {
    // Parse command line arguments
    int port = 8080;
    std::string walPath = "data/wal.log";
    // Security limits
    const size_t MAX_KEY_SIZE = 256;      // 256 bytes max key
    const size_t MAX_VALUE_SIZE = 1048576; // 1MB max value
    const size_t MAX_BODY_SIZE = 1100000;  // slightly above value limit
    SentinelDB::initLogger();
    spdlog::info("SentinelDB starting up");
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--wal" && i + 1 < argc) {
            walPath = argv[++i];
        } else if (arg == "--help") {
            spdlog::info(
                "Usage: {} [OPTIONS]\n"
                "Options:\n"
                "  --port <num>    HTTP port (default: 8080)\n"
                "  --wal <path>    WAL file path (default: data/wal.log)\n"
                "  --help          Show this help",
                argv[0]);
            return 0;
        }
    }

    // Ensure WAL directory exists
    {
        size_t lastSlash = walPath.find_last_of("/\\");
        if (lastSlash != std::string::npos) {
            std::string dirPath = walPath.substr(0, lastSlash);
            struct stat st;
            if (stat(dirPath.c_str(), &st) != 0) {
                mkdir(dirPath.c_str(), 0755);
            }
        }
    }
    
    // Initialize KVStore with optional WAL
    std::shared_ptr<WAL> wal;
    if (!walPath.empty()) {
        wal = std::make_shared<WAL>(walPath);
        Status walStatus = wal->initialize();
        spdlog::info("WAL initialized path={}", walPath);
        spdlog::info("WAL enabled path={}", walPath);
        
        // Replay WAL if it exists
        if (walStatus == Status::OK) {
            std::vector<std::string> snapshotCommands = wal->readSnapshot();
            std::vector<std::string> walCommands = wal->readLog();
            
            if (!snapshotCommands.empty() || !walCommands.empty()) {
                spdlog::info("Replaying WAL and snapshot");
            }
        }
    }
    
    auto kvstore = std::make_shared<KVStore>(wal);
    
    // Replay snapshot and WAL after creating kvstore
    if (wal && wal->isEnabled()) {
        kvstore->setWalEnabled(false);
        
        // Replay snapshot first
        std::vector<std::string> snapshotCommands = wal->readSnapshot();
        if (!snapshotCommands.empty()) {
            for (const auto& cmdLine : snapshotCommands) {
                std::istringstream iss(cmdLine);
                std::string cmdType;
                iss >> cmdType;
                
                if (cmdType == "POLICY") {
                    std::string subCmd, policyName;
                    iss >> subCmd >> policyName;
                    if (subCmd == "SET") {
                        if (policyName == "DEV_FRIENDLY") {
                            kvstore->setDecisionPolicy(DecisionPolicy::DEV_FRIENDLY);
                        } else if (policyName == "SAFE_DEFAULT") {
                            kvstore->setDecisionPolicy(DecisionPolicy::SAFE_DEFAULT);
                        } else if (policyName == "STRICT") {
                            kvstore->setDecisionPolicy(DecisionPolicy::STRICT);
                        }
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
                                    spdlog::info("[WAL Replay] Restored RANGE_INT guard: {}", name);
                                } else {
                                    spdlog::info("[WAL Replay] Skipped duplicate RANGE_INT guard: {}", name);
                                }
                                
                            } else if (guardType == "ENUM") {
                                std::vector<std::string> values;
                                std::string value;
                                while (iss >> value) {
                                    values.push_back(value);
                                }
                                if (!values.empty()) {
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<EnumGuard>(name, keyPattern, values);
                                        kvstore->addGuard(guard);
                                        spdlog::info("[WAL Replay] Restored ENUM guard: {} with {} values", name, values.size());
                                    } else {
                                        spdlog::info("[WAL Replay] Skipped duplicate ENUM guard: {}", name);
                                    }
                                }
                                
                            } else if (guardType == "LENGTH") {
                                size_t min, max;
                                iss >> min >> max;
                                if (!kvstore->hasGuard(name)) {
                                    guard = std::make_shared<LengthGuard>(name, keyPattern, min, max);
                                    kvstore->addGuard(guard);
                                    spdlog::info("[WAL Replay] Restored LENGTH guard: {}", name);
                                } else {
                                    spdlog::info("[WAL Replay] Skipped duplicate LENGTH guard: {}", name);
                                }
                            } else if (guardType == "REGEX") {
                                std::string pattern;
                                iss >> std::quoted(pattern);
                                if (!pattern.empty()) {
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<RegexGuard>(name, keyPattern, pattern);
                                        kvstore->addGuard(guard);
                                        spdlog::info("[WAL Replay] Restored REGEX guard: {}", name);
                                    } else {
                                        spdlog::info("[WAL Replay] Skipped duplicate REGEX guard: {}", name);
                                    }
                                }
                            } else if (guardType == "RATE_CHANGE") {
                                double maxPercent = 0.0;
                                iss >> maxPercent;
                                if (!kvstore->hasGuard(name)) {
                                    guard = std::make_shared<RateChangeGuard>(name, keyPattern, maxPercent);
                                    kvstore->addGuard(guard);
                                    spdlog::info("[WAL Replay] Restored RATE_CHANGE guard: {}", name);
                                } else {
                                    spdlog::info("[WAL Replay] Skipped duplicate RATE_CHANGE guard: {}", name);
                                }
                            } else if (guardType == "CROSS_KEY") {
                                std::string otherKey;
                                double factor = 0.0;
                                iss >> otherKey >> factor;
                                if (!otherKey.empty()) {
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<CrossKeyGuard>(name, keyPattern, otherKey, factor);
                                        kvstore->addGuard(guard);
                                        spdlog::info("[WAL Replay] Restored CROSS_KEY guard: {}", name);
                                    } else {
                                        spdlog::info("[WAL Replay] Skipped duplicate CROSS_KEY guard: {}", name);
                                    }
                                }
                            }
                        } catch (const std::exception& e) {
                            spdlog::warn("[WAL Replay] Failed to restore guard {}: {}", name, e.what());
                        }
                    }
                } else if (cmdType == "SET") {
                    std::string key, value;
                    iss >> key >> value;
                    kvstore->setAtTime(key, value, std::chrono::system_clock::now());
                }
            }
        }
        
        // Replay WAL commands
        std::vector<std::string> commands = wal->readLog();
        if (!commands.empty()) {
            // Phase 1: Replay POLICY and GUARD commands
            for (const auto& cmdLine : commands) {
                std::istringstream iss(cmdLine);
                std::string cmdType;
                iss >> cmdType;
                
                if (cmdType == "POLICY") {
                    std::string subCmd, policyName;
                    iss >> subCmd >> policyName;
                    if (subCmd == "SET") {
                        if (policyName == "DEV_FRIENDLY") {
                            kvstore->setDecisionPolicy(DecisionPolicy::DEV_FRIENDLY);
                        } else if (policyName == "SAFE_DEFAULT") {
                            kvstore->setDecisionPolicy(DecisionPolicy::SAFE_DEFAULT);
                        } else if (policyName == "STRICT") {
                            kvstore->setDecisionPolicy(DecisionPolicy::STRICT);
                        }
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
                                    spdlog::info("[WAL Replay] Restored RANGE_INT guard: {}", name);
                                } else {
                                    spdlog::info("[WAL Replay] Skipped duplicate RANGE_INT guard: {}", name);
                                }
                                
                            } else if (guardType == "ENUM") {
                                std::vector<std::string> values;
                                std::string value;
                                while (iss >> value) {
                                    values.push_back(value);
                                }
                                if (!values.empty()) {
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<EnumGuard>(name, keyPattern, values);
                                        kvstore->addGuard(guard);
                                        spdlog::info("[WAL Replay] Restored ENUM guard: {} with {} values", name, values.size());
                                    } else {
                                        spdlog::info("[WAL Replay] Skipped duplicate ENUM guard: {}", name);
                                    }
                                }
                                
                            } else if (guardType == "LENGTH") {
                                size_t min, max;
                                iss >> min >> max;
                                if (!kvstore->hasGuard(name)) {
                                    guard = std::make_shared<LengthGuard>(name, keyPattern, min, max);
                                    kvstore->addGuard(guard);
                                    spdlog::info("[WAL Replay] Restored LENGTH guard: {}", name);
                                } else {
                                    spdlog::info("[WAL Replay] Skipped duplicate LENGTH guard: {}", name);
                                }
                            } else if (guardType == "REGEX") {
                                std::string pattern;
                                iss >> std::quoted(pattern);
                                if (!pattern.empty()) {
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<RegexGuard>(name, keyPattern, pattern);
                                        kvstore->addGuard(guard);
                                        spdlog::info("[WAL Replay] Restored REGEX guard: {}", name);
                                    } else {
                                        spdlog::info("[WAL Replay] Skipped duplicate REGEX guard: {}", name);
                                    }
                                }
                            } else if (guardType == "RATE_CHANGE") {
                                double maxPercent = 0.0;
                                iss >> maxPercent;
                                if (!kvstore->hasGuard(name)) {
                                    guard = std::make_shared<RateChangeGuard>(name, keyPattern, maxPercent);
                                    kvstore->addGuard(guard);
                                    spdlog::info("[WAL Replay] Restored RATE_CHANGE guard: {}", name);
                                } else {
                                    spdlog::info("[WAL Replay] Skipped duplicate RATE_CHANGE guard: {}", name);
                                }
                            } else if (guardType == "CROSS_KEY") {
                                std::string otherKey;
                                double factor = 0.0;
                                iss >> otherKey >> factor;
                                if (!otherKey.empty()) {
                                    if (!kvstore->hasGuard(name)) {
                                        guard = std::make_shared<CrossKeyGuard>(name, keyPattern, otherKey, factor);
                                        kvstore->addGuard(guard);
                                        spdlog::info("[WAL Replay] Restored CROSS_KEY guard: {}", name);
                                    } else {
                                        spdlog::info("[WAL Replay] Skipped duplicate CROSS_KEY guard: {}", name);
                                    }
                                }
                            }
                        } catch (const std::exception& e) {
                            spdlog::warn("[WAL Replay] Failed to restore guard {}: {}", name, e.what());
                        }
                    }
                }
            }
            
            // Phase 2: Replay data commands
            for (const auto& cmdLine : commands) {
                std::istringstream iss(cmdLine);
                std::string cmdType;
                iss >> cmdType;
                
                if (cmdType == "SET") {
                    std::string key, value;
                    long long timestampMs = 0;
                    iss >> std::quoted(key) >> std::quoted(value);
                    
                    if (iss >> timestampMs) {
                        auto timestamp = std::chrono::system_clock::time_point(
                            std::chrono::milliseconds(timestampMs));
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
        }
        
        kvstore->setWalEnabled(true);
    }
    
    // Initialize HTTP server
    httplib::Server svr;
    
    // CORS headers for all responses
    svr.set_default_headers({
        {"Access-Control-Allow-Origin", "*"},
        {"Access-Control-Allow-Methods", "GET, POST, OPTIONS"},
        {"Access-Control-Allow-Headers", "Content-Type"}
    });

    // Optional API key auth — set API_KEY env var to enable
    const char* apiKeyEnv = std::getenv("SENTINEL_API_KEY");
    std::string requiredApiKey = apiKeyEnv ? std::string(apiKeyEnv) : "";

    if (!requiredApiKey.empty()) {
        svr.set_pre_routing_handler([requiredApiKey](const httplib::Request& req,
                                                     httplib::Response& res) {
            // Health check is always public
            if (req.path == "/health") return httplib::Server::HandlerResponse::Unhandled;

            auto it = req.headers.find("X-API-Key");
            if (it == req.headers.end() || it->second != requiredApiKey) {
                res.status = 401;
                res.set_content("{\"error\":\"Unauthorized\"}", "application/json");
                return httplib::Server::HandlerResponse::Handled;
            }
            return httplib::Server::HandlerResponse::Unhandled;
        });
        spdlog::info("API key authentication enabled");
    } else {
        spdlog::warn("No API key set — server is publicly accessible. Set SENTINEL_API_KEY env var.");
    }
    
    // Health check endpoint
    svr.Get("/health", [kvstore, wal](const httplib::Request&, httplib::Response& res) {
        std::string healthJson = "{\"status\":\"ok\",\"keys\":"
            + std::to_string(kvstore->size())
            + ",\"wal_enabled\":"
            + (wal && wal->isEnabled() ? "true" : "false")
            + ",\"version\":\"1.0.0\"}";
        res.set_content(healthJson, "application/json");
    });
    
    // POST /set - Set a key-value pair
    svr.Post("/set", [kvstore, walPath, MAX_BODY_SIZE, MAX_KEY_SIZE, MAX_VALUE_SIZE](const httplib::Request& req, httplib::Response& res) {
        RequestTimer timer("/set");
        // Input validation
        if (req.body.size() > MAX_BODY_SIZE) {
            Metrics::instance().recordRequest("/set", "error");
            res.status = 413;
            res.set_content("{\"error\":\"Request too large\"}", "application/json");
            return;
        }
        try {
            auto params = parseRequestJSON(req.body);
            std::string key = requireStringField(params, "key");
            auto valueIt = params.find("value");
            if (valueIt == params.end()) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' or 'value' parameter\"}", "application/json");
                return;
            }
            std::string value = jsonValueToStoredString(*valueIt);

            if (key.size() > MAX_KEY_SIZE) {
                Metrics::instance().recordRequest("/set", "error");
                res.status = 400;
                res.set_content("{\"error\":\"Key too long (max 256 bytes)\"}", "application/json");
                return;
            }
            if (value.size() > MAX_VALUE_SIZE) {
                Metrics::instance().recordRequest("/set", "error");
                res.status = 400;
                res.set_content("{\"error\":\"Value too large (max 1MB)\"}", "application/json");
                return;
            }
            spdlog::debug("SET key={} value_size={}", key, value.size());
            
            Status status = kvstore->set(key, value);
            
            if (status == Status::OK) {
                Metrics::instance().recordRequest("/set", "ok");
                Metrics::instance().setActiveKeys(kvstore->size());
                // Update WAL size metric
                {
                    struct stat walStat;
                    if (stat(walPath.c_str(), &walStat) == 0) {
                        Metrics::instance().setWalSize(static_cast<size_t>(walStat.st_size));
                    }
                }
                spdlog::info("SET key={} status=ok", key);
                std::stringstream json;
                json << "{\"status\":\"ok\",\"message\":\"Key '" << escapeJSON(key) << "' set successfully\"}";
                res.set_content(json.str(), "application/json");
            } else {
                res.status = 500;
                Metrics::instance().recordRequest("/set", "error");
                spdlog::warn("SET key={} status=error", key);
                res.set_content("{\"error\":\"Failed to set key\"}", "application/json");
            }
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // GET /get?key=<key> - Get current value
    svr.Get("/get", [kvstore](const httplib::Request& req, httplib::Response& res) {
        RequestTimer timer("/get");
        try {
            if (!req.has_param("key")) {
                res.status = 400;
                Metrics::instance().recordRequest("/get", "not_found");
                res.set_content("{\"error\":\"Missing 'key' parameter\"}", "application/json");
                return;
            }
            
            std::string key = req.get_param_value("key");
            auto value = kvstore->get(key);
            
            if (value.has_value()) {
                Metrics::instance().recordRequest("/get", "ok");
                std::stringstream json;
                json << "{\"key\":\"" << escapeJSON(key) << "\",\"value\":\"" 
                     << escapeJSON(value.value()) << "\"}";
                res.set_content(json.str(), "application/json");
            } else {
                res.status = 404;
                Metrics::instance().recordRequest("/get", "not_found");
                std::stringstream json;
                json << "{\"error\":\"Key not found\",\"key\":\"" << escapeJSON(key) << "\"}";
                res.set_content(json.str(), "application/json");
            }
        } catch (const std::exception& e) {
            res.status = 400;
            Metrics::instance().recordRequest("/get", "not_found");
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // GET /getAt?key=<key>&timestamp=<timestamp> - Get value at specific time
    svr.Get("/getAt", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            if (!req.has_param("key") || !req.has_param("timestamp")) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' or 'timestamp' parameter\"}", "application/json");
                return;
            }
            
            std::string key = req.get_param_value("key");
            std::string timestampStr = req.get_param_value("timestamp");
            
            auto timestamp = parseTimestamp(timestampStr);
            auto value = kvstore->getAtTime(key, timestamp);
            
            if (value.has_value()) {
                std::stringstream json;
                json << "{\"key\":\"" << escapeJSON(key) << "\",\"value\":\"" 
                     << escapeJSON(value.value()) << "\",\"timestamp\":\"" 
                     << escapeJSON(timestampStr) << "\"}";
                res.set_content(json.str(), "application/json");
            } else {
                res.status = 404;
                std::stringstream json;
                json << "{\"error\":\"No version found at or before timestamp\",\"key\":\"" 
                     << escapeJSON(key) << "\",\"timestamp\":\"" << escapeJSON(timestampStr) << "\"}";
                res.set_content(json.str(), "application/json");
            }
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // GET /history?key=<key> - Get version history
    svr.Get("/history", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            if (!req.has_param("key")) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' parameter\"}", "application/json");
                return;
            }
            
            std::string key = req.get_param_value("key");
            auto history = kvstore->getHistory(key);
            
            std::stringstream json;
            json << "{\"key\":\"" << escapeJSON(key) << "\",\"versions\":[";
            
            for (size_t i = 0; i < history.size(); ++i) {
                if (i > 0) json << ",";
                json << "{\"timestamp\":\"" << escapeJSON(formatTimestamp(history[i].timestamp)) 
                     << "\",\"value\":\"" << escapeJSON(history[i].value) << "\"}";
            }
            
            json << "]}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });

    // GET /diff?key=<key>&t1=<timestamp>&t2=<timestamp> - Diff value between two times
    svr.Get("/diff", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            if (!req.has_param("key") || !req.has_param("t1") || !req.has_param("t2")) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key', 't1', or 't2' parameter\"}", "application/json");
                return;
            }

            std::string key = req.get_param_value("key");
            std::string t1Str = req.get_param_value("t1");
            std::string t2Str = req.get_param_value("t2");

            auto t1 = parseTimestamp(t1Str);
            auto t2 = parseTimestamp(t2Str);
            auto diff = kvstore->diffAtTime(key, t1, t2);

            std::stringstream json;
            json << "{\"key\":\"" << escapeJSON(diff.key)
                 << "\",\"from\":\"" << escapeJSON(t1Str)
                 << "\",\"to\":\"" << escapeJSON(t2Str)
                 << "\",\"changed\":" << (diff.changed ? "true" : "false") << ",";

            json << "\"old_value\":";
            if (diff.oldValue.has_value()) {
                json << "\"" << escapeJSON(diff.oldValue.value()) << "\"";
            } else {
                json << "null";
            }
            json << ",\"new_value\":";
            if (diff.newValue.has_value()) {
                json << "\"" << escapeJSON(diff.newValue.value()) << "\"";
            } else {
                json << "null";
            }

            if (diff.hasEvaluation) {
                json << ",\"evaluation\":{";
                appendWriteEvaluationJSON(json, diff.key, diff.newValue.value(), diff.evaluation);
                json << "}";
            }

            json << "}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });

    // GET /audit?key=<key>&since=<timestamp> - Audit log for negotiated writes
    svr.Get("/audit", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            if (!req.has_param("key") || !req.has_param("since")) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' or 'since' parameter\"}", "application/json");
                return;
            }

            std::string key = req.get_param_value("key");
            std::string sinceStr = req.get_param_value("since");
            auto since = parseTimestamp(sinceStr);
            std::optional<std::chrono::system_clock::time_point> sinceOpt = since;

            auto events = kvstore->getAuditEvents(key, sinceOpt);

            std::stringstream json;
            json << "{\"key\":\"" << escapeJSON(key) << "\",\"since\":\""
                 << escapeJSON(sinceStr) << "\",\"events\":[";

            for (size_t i = 0; i < events.size(); ++i) {
                if (i > 0) json << ",";
                const auto& event = events[i];
                json << "{\"timestamp\":\"" << escapeJSON(formatTimestamp(event.timestamp))
                     << "\",\"key\":\"" << escapeJSON(event.key)
                     << "\",\"original_value\":\"" << escapeJSON(event.originalValue)
                     << "\",\"guards_fired\":[";

                for (size_t g = 0; g < event.guardsFired.size(); ++g) {
                    if (g > 0) json << ",";
                    json << "\"" << escapeJSON(event.guardsFired[g]) << "\"";
                }
                json << "],\"policy_used\":\"" << escapeJSON(decisionPolicyToString(event.policyUsed))
                     << "\",\"alternatives_offered\":[";

                for (size_t a = 0; a < event.alternativesOffered.size(); ++a) {
                    if (a > 0) json << ",";
                    const auto& alt = event.alternativesOffered[a];
                    json << "{\"value\":\"" << escapeJSON(alt.value)
                         << "\",\"confidence\":" << formatConfidence(alt.confidence)
                         << ",\"risk\":\"" << escapeJSON(riskLevelToString(alt.risk))
                         << "\",\"reason\":\"" << escapeJSON(alt.reason) << "\"}";
                }
                json << "]";

                json << ",\"final_value\":";
                if (event.finalValue.has_value()) {
                    json << "\"" << escapeJSON(event.finalValue.value()) << "\"";
                } else {
                    json << "null";
                }

                json << ",\"outcome\":\"" << escapeJSON(event.outcome) << "\"}";
            }

            json << "]}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });

    // POST /rollback - Roll back a key to a timestamp with guard evaluation
    svr.Post("/rollback", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            auto params = parseRequestJSON(req.body);
            std::string key = requireStringField(params, "key");
            std::string timestampStr = requireStringField(params, "timestamp");

            auto timestamp = parseTimestamp(timestampStr);
            auto result = kvstore->rollbackToTime(key, timestamp);

            if (!result.found) {
                res.status = 404;
                res.set_content("{\"error\":\"No version found at or before timestamp\"}", "application/json");
                return;
            }

            std::stringstream json;
            json << "{\"key\":\"" << escapeJSON(result.key)
                 << "\",\"timestamp\":\"" << escapeJSON(timestampStr)
                 << "\",\"value\":\"" << escapeJSON(result.rollbackValue.value()) << "\""
                 << ",\"committed\":" << (result.committed ? "true" : "false")
                 << ",\"result\":\"" << guardResultToString(result.evaluation.result) << "\"";

            if (result.hasSuggestedValue) {
                json << ",\"suggested_value\":\"" << escapeJSON(result.suggestedValue.value()) << "\"";
            }

            json << ",\"evaluation\":{";
            appendWriteEvaluationJSON(json, result.key, result.rollbackValue.value(), result.evaluation);
            json << "}}";

            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // GET /explain?key=<key>&timestamp=<timestamp> - Explain temporal query
    svr.Get("/explain", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            if (!req.has_param("key") || !req.has_param("timestamp")) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' or 'timestamp' parameter\"}", "application/json");
                return;
            }
            
            std::string key = req.get_param_value("key");
            std::string timestampStr = req.get_param_value("timestamp");
            
            auto timestamp = parseTimestamp(timestampStr);
            auto result = kvstore->explainGetAtTime(key, timestamp);
            
            std::stringstream json;
            json << "{\"query\":{\"key\":\"" << escapeJSON(result.key) 
                 << "\",\"timestamp\":\"" << escapeJSON(formatTimestamp(result.queryTimestamp)) << "\"},";
            json << "\"found\":" << (result.found ? "true" : "false") << ",";
            json << "\"totalVersions\":" << result.totalVersions << ",";
            
            if (result.found && result.selectedVersion.has_value()) {
                const auto& selected = result.selectedVersion.value();
                json << "\"selectedVersion\":{\"timestamp\":\"" 
                     << escapeJSON(formatTimestamp(selected.timestamp))
                     << "\",\"value\":\"" << escapeJSON(selected.value) << "\"},";
            } else {
                json << "\"selectedVersion\":null,";
            }
            
            json << "\"reasoning\":\"" << escapeJSON(result.reasoning) << "\",";
            json << "\"skippedVersions\":[";
            
            for (size_t i = 0; i < result.skippedVersions.size(); ++i) {
                if (i > 0) json << ",";
                const auto& version = result.skippedVersions[i];
                json << "{\"timestamp\":\"" << escapeJSON(formatTimestamp(version.timestamp))
                     << "\",\"value\":\"" << escapeJSON(version.value) << "\"}";
            }
            
            json << "]}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // POST /propose - Propose a write and get evaluation
    svr.Post("/propose", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            auto params = parseRequestJSON(req.body);
            std::string key = requireStringField(params, "key");
            auto valueIt = params.find("value");
            if (valueIt == params.end()) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' or 'value' parameter\"}", "application/json");
                return;
            }
            std::string value = jsonValueToStoredString(*valueIt);
            
            spdlog::info("[HTTP] POST /propose - Evaluating write: {} = {}", key, value);
            
            // Evaluate the proposed write
            auto evaluation = kvstore->proposeSet(key, value);
            std::string resultStr = guardResultToString(evaluation.result);
            spdlog::info("[HTTP] POST /propose - Result: {} ({} alternative(s))", resultStr, evaluation.alternatives.size());
            
            std::stringstream json;
            json << "{";
            appendWriteEvaluationJSON(json, key, value, evaluation);
            json << "}";
            
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });

    // POST /safe_set - Negotiate and commit a write in one server-side round trip
    svr.Post("/safe_set", [kvstore, walPath, MAX_BODY_SIZE, MAX_KEY_SIZE, MAX_VALUE_SIZE](const httplib::Request& req, httplib::Response& res) {
        RequestTimer timer("/safe_set");
        if (req.body.size() > MAX_BODY_SIZE) {
            Metrics::instance().recordRequest("/safe_set", "error");
            res.status = 413;
            res.set_content("{\"error\":\"Request too large\"}", "application/json");
            return;
        }

        try {
            auto params = parseRequestJSON(req.body);
            std::string key = requireStringField(params, "key");
            auto valueIt = params.find("value");
            if (valueIt == params.end()) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'key' or 'value' parameter\"}", "application/json");
                return;
            }
            std::string value = jsonValueToStoredString(*valueIt);

            if (key.size() > MAX_KEY_SIZE) {
                Metrics::instance().recordRequest("/safe_set", "error");
                res.status = 400;
                res.set_content("{\"error\":\"Key too long (max 256 bytes)\"}", "application/json");
                return;
            }
            if (value.size() > MAX_VALUE_SIZE) {
                Metrics::instance().recordRequest("/safe_set", "error");
                res.status = 400;
                res.set_content("{\"error\":\"Value too large (max 1MB)\"}", "application/json");
                return;
            }

            spdlog::info("[HTTP] POST /safe_set - Negotiating write: {} = {}", key, value);
            auto negotiation = kvstore->safeSet(key, value);

            std::stringstream json;
            json << "{";
            appendWriteEvaluationJSON(json, key, value, negotiation.evaluation);
            json << ",\"committed\":" << (negotiation.committed ? "true" : "false") << ",";
            if (negotiation.committed) {
                json << "\"storedValue\":\"" << escapeJSON(negotiation.storedValue) << "\"";
            } else {
                json << "\"storedValue\":null";
            }
            json << "}";

            if (!negotiation.committed) {
                Metrics::instance().recordRequest("/safe_set", "error");
                res.status = 409;
                res.set_content(json.str(), "application/json");
                return;
            }

            Metrics::instance().recordRequest("/safe_set", "ok");
            Metrics::instance().setActiveKeys(kvstore->size());
            {
                struct stat walStat;
                if (stat(walPath.c_str(), &walStat) == 0) {
                    Metrics::instance().setWalSize(static_cast<size_t>(walStat.st_size));
                }
            }

            spdlog::info("[HTTP] POST /safe_set - Stored value for key {} as {}", key, negotiation.storedValue);
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            Metrics::instance().recordRequest("/safe_set", "error");
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });

    // POST /plan_commit - Plan a batch of writes without committing
    svr.Post("/plan_commit", [kvstore, MAX_BODY_SIZE, MAX_KEY_SIZE, MAX_VALUE_SIZE](const httplib::Request& req, httplib::Response& res) {
        RequestTimer timer("/plan_commit");
        if (req.body.size() > MAX_BODY_SIZE) {
            Metrics::instance().recordRequest("/plan_commit", "error");
            res.status = 413;
            res.set_content("{\"error\":\"Request too large\"}", "application/json");
            return;
        }

        try {
            auto params = parseRequestJSON(req.body);
            auto writesIt = params.find("writes");
            if (writesIt == params.end() || !writesIt->is_array()) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'writes' array\"}", "application/json");
                return;
            }

            std::vector<std::pair<std::string, std::string>> writes;
            for (const auto& entry : *writesIt) {
                if (!entry.is_object()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"Each write must be an object\"}", "application/json");
                    return;
                }

                if (entry.find("key") == entry.end() || entry.find("value") == entry.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"Each write requires 'key' and 'value'\"}", "application/json");
                    return;
                }

                std::string key = entry.at("key").get<std::string>();
                std::string value = jsonValueToStoredString(entry.at("value"));

                if (key.size() > MAX_KEY_SIZE) {
                    Metrics::instance().recordRequest("/plan_commit", "error");
                    res.status = 400;
                    res.set_content("{\"error\":\"Key too long (max 256 bytes)\"}", "application/json");
                    return;
                }
                if (value.size() > MAX_VALUE_SIZE) {
                    Metrics::instance().recordRequest("/plan_commit", "error");
                    res.status = 400;
                    res.set_content("{\"error\":\"Value too large (max 1MB)\"}", "application/json");
                    return;
                }

                writes.emplace_back(key, value);
            }

            auto plan = kvstore->planBatch(writes);

            std::stringstream json;
            json << "{\"canCommit\":" << (plan.canCommit ? "true" : "false") << ",";
            json << "\"items\":[";
            for (size_t i = 0; i < plan.items.size(); ++i) {
                if (i > 0) json << ",";
                const auto& item = plan.items[i];
                json << "{\"key\":\"" << escapeJSON(item.key) << "\""
                     << ",\"proposedValue\":\"" << escapeJSON(item.proposedValue) << "\""
                     << ",\"result\":\"" << guardResultToString(item.evaluation.result) << "\""
                     << ",\"rewritten\":" << (item.rewritten ? "true" : "false")
                     << ",\"finalValue\":";
                if (item.hasFinalValue) {
                    json << "\"" << escapeJSON(item.finalValue) << "\"";
                } else {
                    json << "null";
                }
                json << ",\"evaluation\":{";
                appendWriteEvaluationJSON(json, item.key, item.proposedValue, item.evaluation);
                json << "}}";
            }
            json << "],\"finalTransaction\":[";
            for (size_t i = 0; i < plan.finalWrites.size(); ++i) {
                if (i > 0) json << ",";
                const auto& write = plan.finalWrites[i];
                json << "{\"key\":\"" << escapeJSON(write.first)
                     << "\",\"value\":\"" << escapeJSON(write.second) << "\"}";
            }
            json << "]}";

            Metrics::instance().recordRequest("/plan_commit", "ok");
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            Metrics::instance().recordRequest("/plan_commit", "error");
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });

    // POST /commit_batch - Commit a batch of writes atomically
    svr.Post("/commit_batch", [kvstore, MAX_BODY_SIZE, MAX_KEY_SIZE, MAX_VALUE_SIZE](const httplib::Request& req, httplib::Response& res) {
        RequestTimer timer("/commit_batch");
        if (req.body.size() > MAX_BODY_SIZE) {
            Metrics::instance().recordRequest("/commit_batch", "error");
            res.status = 413;
            res.set_content("{\"error\":\"Request too large\"}", "application/json");
            return;
        }

        try {
            auto params = parseRequestJSON(req.body);
            auto writesIt = params.find("writes");
            if (writesIt == params.end() || !writesIt->is_array()) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing 'writes' array\"}", "application/json");
                return;
            }

            std::vector<std::pair<std::string, std::string>> writes;
            for (const auto& entry : *writesIt) {
                if (!entry.is_object()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"Each write must be an object\"}", "application/json");
                    return;
                }

                if (entry.find("key") == entry.end() || entry.find("value") == entry.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"Each write requires 'key' and 'value'\"}", "application/json");
                    return;
                }

                std::string key = entry.at("key").get<std::string>();
                std::string value = jsonValueToStoredString(entry.at("value"));

                if (key.size() > MAX_KEY_SIZE) {
                    Metrics::instance().recordRequest("/commit_batch", "error");
                    res.status = 400;
                    res.set_content("{\"error\":\"Key too long (max 256 bytes)\"}", "application/json");
                    return;
                }
                if (value.size() > MAX_VALUE_SIZE) {
                    Metrics::instance().recordRequest("/commit_batch", "error");
                    res.status = 400;
                    res.set_content("{\"error\":\"Value too large (max 1MB)\"}", "application/json");
                    return;
                }

                writes.emplace_back(key, value);
            }

            Status status = kvstore->commitBatch(writes);
            if (status != Status::OK) {
                Metrics::instance().recordRequest("/commit_batch", "error");
                res.status = 500;
                res.set_content("{\"error\":\"Failed to commit batch\"}", "application/json");
                return;
            }

            Metrics::instance().recordRequest("/commit_batch", "ok");
            Metrics::instance().setActiveKeys(kvstore->size());

            std::stringstream json;
            json << "{\"status\":\"ok\",\"count\":" << writes.size() << "}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            Metrics::instance().recordRequest("/commit_batch", "error");
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // GET /guards - List all guards
    svr.Get("/guards", [kvstore](const httplib::Request&, httplib::Response& res) {
        try {
            const auto& guards = kvstore->getGuards();
            spdlog::info("[HTTP] GET /guards - Retrieved {} guard(s)", guards.size());
            
            std::stringstream json;
            json << "{\"guards\":[";
            
            for (size_t i = 0; i < guards.size(); ++i) {
                if (i > 0) json << ",";
                const auto& guard = guards[i];
                json << "{\"name\":\"" << escapeJSON(guard->getName())
                     << "\",\"keyPattern\":\"" << escapeJSON(guard->getKeyPattern())
                     << "\",\"description\":\"" << escapeJSON(guard->describe())
                     << "\",\"enabled\":" << (guard->isEnabled() ? "true" : "false") << "}";
            }
            
            json << "]}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            std::stringstream json;
            json << "{\"error\":\"" << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // POST /guards - Add a new guard constraint
    // FIX: This endpoint was previously missing, causing guards to not be registerable via HTTP.
    // Now properly parses JSON, constructs Guard objects, and calls kvstore->addGuard().
    // Expected JSON formats:
    // RANGE_INT: {"type":"RANGE_INT","name":"guard_name","keyPattern":"key*","min":"0","max":"100"}
    // ENUM:      {"type":"ENUM","name":"guard_name","keyPattern":"key*","values":"val1,val2,val3"}
    // LENGTH:    {"type":"LENGTH","name":"guard_name","keyPattern":"key*","min":"1","max":"50"}
    // REGEX:     {"type":"REGEX","name":"guard_name","keyPattern":"key*","pattern":"^.+@.+\\..+$"}
    // RATE:      {"type":"RATE_CHANGE","name":"guard_name","keyPattern":"key*","maxPercent":"5"}
    // CROSS:     {"type":"CROSS_KEY","name":"guard_name","keyPattern":"key*","otherKey":"ref","factor":"1.25"}
    svr.Post("/guards", [kvstore, wal](const httplib::Request& req, httplib::Response& res) {
        try {
            auto params = parseRequestJSON(req.body);
            
            spdlog::info("[HTTP] POST /guards - Received guard registration request");
            
            // Validate required fields
            if (params.find("type") == params.end() ||
                params.find("name") == params.end() ||
                params.find("keyPattern") == params.end()) {
                res.status = 400;
                res.set_content("{\"error\":\"Missing required fields: type, name, keyPattern\"}", "application/json");
                return;
            }
            
            std::string type = requireStringField(params, "type");
            std::string name = requireStringField(params, "name");
            std::string keyPattern = requireStringField(params, "keyPattern");
            
            // Convert type to uppercase for consistency
            std::transform(type.begin(), type.end(), type.begin(), ::toupper);
            
            std::shared_ptr<Guard> guard;
            std::string description;
            
            if (type == "RANGE_INT" || type == "RANGE") {
                // Parse min and max values
                if (params.find("min") == params.end() || params.find("max") == params.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"RANGE_INT requires 'min' and 'max' fields\"}", "application/json");
                    return;
                }
                
                int min = requireIntField(params, "min");
                int max = requireIntField(params, "max");
                
                guard = std::make_shared<RangeIntGuard>(name, keyPattern, min, max);
                description = "RANGE_INT [" + std::to_string(min) + ", " + std::to_string(max) + "]";
                
            } else if (type == "ENUM") {
                // Parse values array from JSON
                // Simple parser expects values as comma-separated string
                if (params.find("values") == params.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"ENUM requires 'values' field (comma-separated string or JSON array)\"}", "application/json");
                    return;
                }
                
                std::vector<std::string> values = parseEnumValues(params);
                
                if (values.empty()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"ENUM requires at least one value\"}", "application/json");
                    return;
                }
                
                guard = std::make_shared<EnumGuard>(name, keyPattern, values);
                description = "ENUM with " + std::to_string(values.size()) + " value(s)";
                
            } else if (type == "LENGTH") {
                // Parse min and max length
                if (params.find("min") == params.end() || params.find("max") == params.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"LENGTH requires 'min' and 'max' fields\"}", "application/json");
                    return;
                }
                
                size_t min = requireSizeField(params, "min");
                size_t max = requireSizeField(params, "max");
                
                guard = std::make_shared<LengthGuard>(name, keyPattern, min, max);
                description = "LENGTH [" + std::to_string(min) + ", " + std::to_string(max) + "] characters";
            } else if (type == "REGEX") {
                if (params.find("pattern") == params.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"REGEX requires 'pattern' field\"}", "application/json");
                    return;
                }

                std::string pattern = requireStringField(params, "pattern");
                guard = std::make_shared<RegexGuard>(name, keyPattern, pattern);
                description = "REGEX " + pattern;
            } else if (type == "RATE_CHANGE") {
                if (params.find("maxPercent") == params.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"RATE_CHANGE requires 'maxPercent' field\"}", "application/json");
                    return;
                }

                double maxPercent = requireDoubleField(params, "maxPercent");
                guard = std::make_shared<RateChangeGuard>(name, keyPattern, maxPercent);
                description = "RATE_CHANGE max " + std::to_string(maxPercent) + "%";
            } else if (type == "CROSS_KEY") {
                if (params.find("otherKey") == params.end() || params.find("factor") == params.end()) {
                    res.status = 400;
                    res.set_content("{\"error\":\"CROSS_KEY requires 'otherKey' and 'factor' fields\"}", "application/json");
                    return;
                }

                std::string otherKey = requireStringField(params, "otherKey");
                double factor = requireDoubleField(params, "factor");
                guard = std::make_shared<CrossKeyGuard>(name, keyPattern, otherKey, factor);
                description = "CROSS_KEY " + otherKey + " * " + std::to_string(factor);
                
            } else {
                res.status = 400;
                res.set_content("{\"error\":\"Invalid guard type. Use RANGE_INT, ENUM, LENGTH, REGEX, RATE_CHANGE, or CROSS_KEY\"}", "application/json");
                return;
            }
            
            // Add guard to kvstore
            kvstore->addGuard(guard);
            
            // Persist to WAL
            if (wal && wal->isEnabled()) {
                std::string walParams;
                if (type == "RANGE_INT" || type == "RANGE") {
                    walParams = std::to_string(requireIntField(params, "min")) + " " +
                                std::to_string(requireIntField(params, "max"));
                } else if (type == "ENUM") {
                    walParams = joinWalValues(parseEnumValues(params));
                } else if (type == "LENGTH") {
                    walParams = std::to_string(requireSizeField(params, "min")) + " " +
                                std::to_string(requireSizeField(params, "max"));
                } else if (type == "REGEX") {
                    std::ostringstream oss;
                    oss << std::quoted(requireStringField(params, "pattern"));
                    walParams = oss.str();
                } else if (type == "RATE_CHANGE") {
                    walParams = std::to_string(requireDoubleField(params, "maxPercent"));
                } else if (type == "CROSS_KEY") {
                    walParams = requireStringField(params, "otherKey") + " " +
                                std::to_string(requireDoubleField(params, "factor"));
                }
                
                Status walStatus = wal->logGuardAdd(type, name, keyPattern, walParams);
                if (walStatus == Status::OK) {
                    spdlog::info("[HTTP] POST /guards - Written to WAL: GUARD ADD {} {}", type, name);
                } else {
                    spdlog::warn("[HTTP] POST /guards - Failed to write guard to WAL");
                }
            }
            
            spdlog::info("[HTTP] POST /guards - Successfully added guard '{}' (type: {}, pattern: {})",
                         name, type, keyPattern);
            
            std::stringstream json;
            json << "{\"status\":\"ok\",\"message\":\"Guard '" << escapeJSON(name) 
                 << "' added successfully\",\"guard\":{"
                 << "\"name\":\"" << escapeJSON(name) << "\","
                 << "\"type\":\"" << escapeJSON(type) << "\","
                 << "\"keyPattern\":\"" << escapeJSON(keyPattern) << "\","
                 << "\"description\":\"" << escapeJSON(description) << "\"}}";
            res.set_content(json.str(), "application/json");
            
        } catch (const std::invalid_argument& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid numeric value: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        } catch (const std::out_of_range& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Numeric value out of range: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            std::stringstream json;
            json << "{\"error\":\"Internal error: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // POST /config/retention - Configure retention policy
    svr.Post("/config/retention", [kvstore](const httplib::Request& req, httplib::Response& res) {
        try {
            auto params = parseRequestJSON(req.body);
            std::string modeStr = requireStringField(params, "mode");
            std::transform(modeStr.begin(), modeStr.end(), modeStr.begin(), ::toupper);
            
            RetentionPolicy policy;
            std::string description;
            
            if (modeStr == "FULL") {
                policy = RetentionPolicy();
                description = "FULL (keep all versions)";
            } else if (modeStr.find("LAST ") == 0) {
                std::string rest = modeStr.substr(5);
                
                // Check if it's time-based (ends with 's')
                if (!rest.empty() && rest.back() == 'S') {
                    // Time-based retention
                    rest.pop_back();
                    if (rest.empty()) {
                        res.status = 400;
                        res.set_content("{\"error\":\"Invalid format, expected number before 's'\"}", "application/json");
                        return;
                    }
                    
                    size_t pos;
                    int seconds = std::stoi(rest, &pos);
                    
                    if (pos != rest.size()) {
                        res.status = 400;
                        res.set_content("{\"error\":\"Invalid seconds value\"}", "application/json");
                        return;
                    }
                    
                    if (seconds <= 0) {
                        res.status = 400;
                        res.set_content("{\"error\":\"Seconds must be positive\"}", "application/json");
                        return;
                    }
                    
                    policy = RetentionPolicy(RetentionMode::LAST_T, seconds);
                    description = "LAST " + std::to_string(seconds) + "s (keep versions from last " + std::to_string(seconds) + " seconds)";
                } else {
                    // Count-based retention
                    size_t pos;
                    int count = std::stoi(rest, &pos);
                    
                    if (pos != rest.size()) {
                        res.status = 400;
                        res.set_content("{\"error\":\"Invalid count value\"}", "application/json");
                        return;
                    }
                    
                    if (count <= 0) {
                        res.status = 400;
                        res.set_content("{\"error\":\"Count must be positive\"}", "application/json");
                        return;
                    }
                    
                    policy = RetentionPolicy(RetentionMode::LAST_N, count);
                    description = "LAST " + std::to_string(count) + " (keep last " + std::to_string(count) + " versions)";
                }
            } else {
                res.status = 400;
                res.set_content("{\"error\":\"Invalid mode. Use 'FULL', 'LAST N', or 'LAST Ts'\"}", "application/json");
                return;
            }
            
            kvstore->setRetentionPolicy(policy);
            
            std::stringstream json;
            json << "{\"status\":\"ok\",\"message\":\"Retention policy set to " 
                 << escapeJSON(description) << "\"}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // GET /policy - Get current decision policy
    svr.Get("/policy", [kvstore](const httplib::Request&, httplib::Response& res) {
        try {
            DecisionPolicy policy = kvstore->getDecisionPolicy();
            std::string policyName;
            std::string description;
            
            switch (policy) {
                case DecisionPolicy::DEV_FRIENDLY:
                    policyName = "DEV_FRIENDLY";
                    description = "Always negotiate guard violations when alternatives exist";
                    break;
                case DecisionPolicy::SAFE_DEFAULT:
                    policyName = "SAFE_DEFAULT";
                    description = "Negotiate low-risk violations (with alternatives), reject high-risk (no alternatives)";
                    break;
                case DecisionPolicy::STRICT:
                    policyName = "STRICT";
                    description = "Reject all guard violations without negotiation";
                    break;
            }
            
            std::stringstream json;
            json << "{\"activePolicy\":\"" << policyName << "\","
                 << "\"description\":\"" << escapeJSON(description) << "\"}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            std::stringstream json;
            json << "{\"error\":\"" << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // POST /policy - Set decision policy
    svr.Post("/policy", [kvstore, wal](const httplib::Request& req, httplib::Response& res) {
        try {
            auto params = parseRequestJSON(req.body);
            std::string policyStr = requireStringField(params, "policy");
            std::transform(policyStr.begin(), policyStr.end(), policyStr.begin(), ::toupper);
            
            spdlog::info("[HTTP] POST /policy - Changing policy to: {}", policyStr);
            
            DecisionPolicy newPolicy;
            if (policyStr == "DEV_FRIENDLY") {
                newPolicy = DecisionPolicy::DEV_FRIENDLY;
            } else if (policyStr == "SAFE_DEFAULT") {
                newPolicy = DecisionPolicy::SAFE_DEFAULT;
            } else if (policyStr == "STRICT") {
                newPolicy = DecisionPolicy::STRICT;
            } else {
                res.status = 400;
                res.set_content("{\"error\":\"Invalid policy. Use DEV_FRIENDLY, SAFE_DEFAULT, or STRICT\"}", "application/json");
                return;
            }
            
            // Set policy in memory (WAL persistence is handled inside kvstore)
            kvstore->setDecisionPolicy(newPolicy);
            
            spdlog::info("[HTTP] POST /policy - Policy changed successfully to {}", policyStr);
            
            std::stringstream json;
            json << "{\"status\":\"ok\",\"activePolicy\":\"" << policyStr << "\"}";
            res.set_content(json.str(), "application/json");
        } catch (const std::exception& e) {
            res.status = 400;
            std::stringstream json;
            json << "{\"error\":\"Invalid request: " << escapeJSON(e.what()) << "\"}";
            res.set_content(json.str(), "application/json");
        }
    });
    
    // Register signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    // Start server in background thread
    spdlog::info("HTTP server listening port={}", port);
    
    // Start server in background thread
    std::thread serverThread([&svr, port]() {
        // Prometheus metrics endpoint
        svr.Get("/metrics", [](const httplib::Request&, httplib::Response& res) {
            RequestTimer timer("/metrics");
            res.set_content(Metrics::instance().toPrometheusFormat(),
                            "text/plain; version=0.0.4");
            Metrics::instance().recordRequest("/metrics", "ok");
        });
        spdlog::info("Metrics endpoint registered path=/metrics");
        svr.listen("0.0.0.0", port);
    });
    
    // Keep main thread alive until shutdown signal
    while (!shutdownRequested.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    // Graceful shutdown
    spdlog::info("SentinelDB shutting down gracefully");
    svr.stop();
    
    if (serverThread.joinable()) {
        serverThread.join();
    }
    
    return 0;
}
