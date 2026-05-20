#include "replay_service.h"
#include <sstream>
#include <iomanip>
#include <algorithm>

namespace {
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

    auto isAllDigits = [](const std::string& text) {
        if (text.empty()) {
            return false;
        }
        return std::all_of(text.begin(), text.end(), [](unsigned char c) {
            return std::isdigit(c);
        });
    };

    if (allowTimestamp && parts.size() > 1 && isAllDigits(parts.back())) {
        try {
            timestampMs = std::stoll(parts.back());
            parts.pop_back();
        } catch (...) {
            timestampMs.reset();
        }
    }

    std::ostringstream oss;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            oss << ' ';
        }
        oss << parts[i];
    }
    value = oss.str();
    return true;
}
}

ReplayStats ReplayService::replay(const std::shared_ptr<KVStore>& kvstore,
                                  const std::shared_ptr<WAL>& wal) {
    ReplayStats stats;
    if (!kvstore || !wal || !wal->isEnabled()) {
        return stats;
    }

    kvstore->setWalEnabled(false);

    std::vector<std::string> snapshotCommands = wal->readSnapshot();
    stats.snapshotCommands = snapshotCommands.size();
    if (!snapshotCommands.empty()) {
        stats.snapshotApplied = true;
        replayConfigCommands(snapshotCommands, kvstore);
        auto snapshotTime = std::chrono::system_clock::now();
        replayDataCommands(snapshotCommands, kvstore, snapshotTime);
        stats.keysAfterSnapshot = kvstore->size();
    }

    std::vector<std::string> commands = wal->readLog();
    stats.walCommands = commands.size();
    if (!commands.empty()) {
        replayConfigCommands(commands, kvstore);
        replayDataCommands(commands, kvstore, std::nullopt);
        stats.keysAfterWal = kvstore->size();
    }

    kvstore->setWalEnabled(true);
    return stats;
}

void ReplayService::replayConfigCommands(const std::vector<std::string>& commands,
                                         const std::shared_ptr<KVStore>& kvstore) {
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

            if (subCmd != "ADD") {
                continue;
            }

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
                } else if (guardType == "REGEX") {
                    std::string pattern;
                    iss >> std::quoted(pattern);
                    if (!pattern.empty() && !kvstore->hasGuard(name)) {
                        guard = std::make_shared<RegexGuard>(name, keyPattern, pattern);
                        kvstore->addGuard(guard);
                    }
                } else if (guardType == "RATE_CHANGE") {
                    double maxPercent = 0.0;
                    iss >> maxPercent;
                    if (!kvstore->hasGuard(name)) {
                        guard = std::make_shared<RateChangeGuard>(name, keyPattern, maxPercent);
                        kvstore->addGuard(guard);
                    }
                } else if (guardType == "CROSS_KEY") {
                    std::string otherKey;
                    double factor = 0.0;
                    iss >> otherKey >> factor;
                    if (!otherKey.empty() && !kvstore->hasGuard(name)) {
                        guard = std::make_shared<CrossKeyGuard>(name, keyPattern, otherKey, factor);
                        kvstore->addGuard(guard);
                    }
                }
            } catch (...) {
                // Ignore invalid guard entries in replay
            }
        }
    }
}

void ReplayService::replayDataCommands(const std::vector<std::string>& commands,
                                       const std::shared_ptr<KVStore>& kvstore,
                                       std::optional<std::chrono::system_clock::time_point> defaultTimestamp) {
    for (const auto& cmdLine : commands) {
        std::istringstream iss(cmdLine);
        std::string cmdType;
        iss >> cmdType;

        if (cmdType == "SET") {
            std::string key, value;
            std::optional<long long> timestampMs;
            bool allowTimestamp = !defaultTimestamp.has_value();
            if (!parseSetLine(cmdLine, key, value, timestampMs, allowTimestamp) || key.empty()) {
                continue;
            }

            if (timestampMs.has_value()) {
                auto timestamp = std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(timestampMs.value()));
                kvstore->setAtTime(key, value, timestamp);
            } else if (defaultTimestamp.has_value()) {
                kvstore->setAtTime(key, value, defaultTimestamp.value());
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
