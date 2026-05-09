#include "kvstore.h"
#include "logger.h"
#include <algorithm>
#include <cmath>
#include <limits>
#include <sstream>
#include <mutex>
#include <shared_mutex>

namespace {
std::string decisionPolicyToString(DecisionPolicy policy) {
    switch (policy) {
        case DecisionPolicy::DEV_FRIENDLY:
            return "DEV_FRIENDLY";
        case DecisionPolicy::SAFE_DEFAULT:
            return "SAFE_DEFAULT";
        case DecisionPolicy::STRICT:
            return "STRICT";
    }
    return "SAFE_DEFAULT";
}

std::string joinStrings(const std::vector<std::string>& values, char separator) {
    std::ostringstream oss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            oss << separator;
        }
        oss << values[i];
    }
    return oss.str();
}

std::optional<long long> parseIntegerStrict(const std::string& value) {
    if (value.empty()) {
        return std::nullopt;
    }

    size_t pos = 0;
    try {
        long long parsed = std::stoll(value, &pos);
        if (pos != value.size()) {
            return std::nullopt;
        }
        return parsed;
    } catch (...) {
        return std::nullopt;
    }
}

std::string formatPercentage(size_t numerator, size_t denominator) {
    if (denominator == 0) {
        return "0";
    }

    std::ostringstream oss;
    double percentage = (static_cast<double>(numerator) * 100.0) / static_cast<double>(denominator);
    oss.setf(std::ios::fixed);
    oss.precision(1);
    oss << percentage;
    std::string result = oss.str();
    while (!result.empty() && result.back() == '0') {
        result.pop_back();
    }
    if (!result.empty() && result.back() == '.') {
        result.pop_back();
    }
    return result;
}

std::pair<long long, long long> smallestCoveringWindow(std::vector<long long> values, size_t coverageCount) {
    std::sort(values.begin(), values.end());
    if (values.empty()) {
        return {0, 0};
    }
    if (coverageCount == 0 || coverageCount >= values.size()) {
        return {values.front(), values.back()};
    }

    size_t bestStart = 0;
    long long bestWidth = std::numeric_limits<long long>::max();
    for (size_t start = 0; start + coverageCount - 1 < values.size(); ++start) {
        size_t end = start + coverageCount - 1;
        long long width = values[end] - values[start];
        if (width < bestWidth) {
            bestWidth = width;
            bestStart = start;
        }
    }

    return {values[bestStart], values[bestStart + coverageCount - 1]};
}
}

KVStore::KVStore(std::shared_ptr<WAL> walPtr) 
    : wal(walPtr), walEnabled(true), decisionPolicy(DecisionPolicy::SAFE_DEFAULT) {}

Status KVStore::set(const std::string& key, const std::string& value) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    return setInternal(key, value);
}

Status KVStore::setInternal(const std::string& key, const std::string& value) {
    // Create version with current timestamp
    auto timestamp = std::chrono::system_clock::now();
    
    // Write to WAL first (if enabled)
    if (walEnabled && wal && wal->isEnabled()) {
        Status walStatus = wal->logSet(key, value, timestamp);
        // Continue even if WAL write fails (warn user but don't crash)
        if (walStatus != Status::OK) {
            // Warning already printed by WAL
        }
    }
    
    // Append new version to in-memory store
    store[key].emplace_back(timestamp, value);
    
    // Apply retention policy
    applyRetention(key);

    touchKey(key);
    evictIfNeeded();

    if (decisionPolicy == DecisionPolicy::DEV_FRIENDLY) {
        recordLearningObservation(key, value);
    }
    
    return Status::OK;
}

Status KVStore::setAtTime(const std::string& key, const std::string& value,
                          std::chrono::system_clock::time_point timestamp) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    // This is for replay - do NOT log to WAL
    // Just add the version with the given timestamp
    store[key].emplace_back(timestamp, value);
    
    // Apply retention policy
    applyRetention(key);
    
    return Status::OK;
}

std::optional<std::string> KVStore::get(const std::string& key) {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    auto it = store.find(key);
    if (it != store.end() && !it->second.empty()) {
        // Return the latest version (last element)
        return it->second.back().value;
    }
    return std::nullopt;
}

Status KVStore::del(const std::string& key) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    auto it = store.find(key);
    if (it != store.end()) {
        // Write to WAL first (if enabled)
        if (walEnabled && wal && wal->isEnabled()) {
            Status walStatus = wal->logDel(key);
            // Continue even if WAL write fails
            if (walStatus != Status::OK) {
                // Warning already printed by WAL
            }
        }
        
        // Remove all versions from in-memory store
        store.erase(it);
        {
            auto lruIt = lruMap_.find(key);
            if (lruIt != lruMap_.end()) {
                lruOrder_.erase(lruIt->second);
                lruMap_.erase(lruIt);
            }
        }
        return Status::OK;
    }
    return Status::NOT_FOUND;
}

std::optional<std::string> KVStore::getAtTime(const std::string& key, 
                                               std::chrono::system_clock::time_point timestamp) {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    auto it = store.find(key);
    if (it == store.end() || it->second.empty()) {
        return std::nullopt;
    }
    
    // Find the latest version at or before the given timestamp
    const auto& versions = it->second;

    auto upper = std::upper_bound(
        versions.begin(),
        versions.end(),
        timestamp,
        [](const auto& ts, const Version& version) {
            return ts < version.timestamp;
        }
    );

    if (upper == versions.begin()) {
        return std::nullopt;
    }

    return std::prev(upper)->value;
}

DiffResult KVStore::diffAtTime(const std::string& key,
                               std::chrono::system_clock::time_point t1,
                               std::chrono::system_clock::time_point t2) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    DiffResult result;
    result.key = key;
    result.from = t1;
    result.to = t2;
    result.changed = false;
    result.hasEvaluation = false;

    auto getValueAt = [&](const std::string& targetKey, std::chrono::system_clock::time_point ts)
        -> std::optional<std::string> {
        auto it = store.find(targetKey);
        if (it == store.end() || it->second.empty()) {
            return std::nullopt;
        }

        const auto& versions = it->second;
        auto upper = std::upper_bound(
            versions.begin(),
            versions.end(),
            ts,
            [](const auto& timestamp, const Version& version) {
                return timestamp < version.timestamp;
            }
        );

        if (upper == versions.begin()) {
            return std::nullopt;
        }

        return std::prev(upper)->value;
    };

    result.oldValue = getValueAt(key, t1);
    result.newValue = getValueAt(key, t2);
    result.changed = result.oldValue != result.newValue;

    if (result.newValue.has_value()) {
        auto storeProvider = [this](const std::string& targetKey) -> std::optional<std::string> {
            auto it = store.find(targetKey);
            if (it != store.end() && !it->second.empty()) {
                return it->second.back().value;
            }
            return std::nullopt;
        };

        auto timeProvider = [&](const std::string& targetKey) -> std::optional<std::string> {
            return getValueAt(targetKey, t1);
        };

        for (const auto& guard : guards) {
            guard->setValueProvider(timeProvider);
        }

        result.evaluation = simulateWrite(key, result.newValue.value());
        applyDecisionPolicy(result.evaluation);
        result.hasEvaluation = true;

        for (const auto& guard : guards) {
            guard->setValueProvider(storeProvider);
        }
    }

    return result;
}

RollbackResult KVStore::rollbackToTime(const std::string& key,
                                       std::chrono::system_clock::time_point timestamp) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    RollbackResult result;
    result.key = key;
    result.targetTimestamp = timestamp;
    result.found = false;
    result.committed = false;
    result.hasSuggestedValue = false;

    auto getValueAt = [&](const std::string& targetKey, std::chrono::system_clock::time_point ts)
        -> std::optional<std::string> {
        auto it = store.find(targetKey);
        if (it == store.end() || it->second.empty()) {
            return std::nullopt;
        }

        const auto& versions = it->second;
        auto upper = std::upper_bound(
            versions.begin(),
            versions.end(),
            ts,
            [](const auto& timepoint, const Version& version) {
                return timepoint < version.timestamp;
            }
        );

        if (upper == versions.begin()) {
            return std::nullopt;
        }

        return std::prev(upper)->value;
    };

    result.rollbackValue = getValueAt(key, timestamp);
    if (!result.rollbackValue.has_value()) {
        return result;
    }

    result.found = true;
    result.evaluation = simulateWrite(key, result.rollbackValue.value());
    applyDecisionPolicy(result.evaluation);

    std::optional<std::string> finalValue;
    if (result.evaluation.result == GuardResult::ACCEPT) {
        finalValue = result.rollbackValue.value();
    } else if (result.evaluation.result == GuardResult::COUNTER_OFFER &&
               !result.evaluation.alternatives.empty()) {
        finalValue = result.evaluation.alternatives.front().value;
    }

    appendAuditEvent(key, result.rollbackValue.value(), result.evaluation, finalValue);

    if (result.evaluation.result == GuardResult::ACCEPT) {
        if (setInternal(key, result.rollbackValue.value()) == Status::OK) {
            result.committed = true;
            result.storedValue = result.rollbackValue;
        }
    } else if (result.evaluation.result == GuardResult::COUNTER_OFFER &&
               !result.evaluation.alternatives.empty()) {
        result.hasSuggestedValue = true;
        result.suggestedValue = result.evaluation.alternatives.front().value;
    }

    return result;
}

ExplainResult KVStore::explainGetAtTime(const std::string& key,
                                         std::chrono::system_clock::time_point timestamp) {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    ExplainResult result;
    result.found = false;
    result.key = key;
    result.queryTimestamp = timestamp;
    result.totalVersions = 0;
    
    auto it = store.find(key);
    if (it == store.end() || it->second.empty()) {
        result.reasoning = "Key not found in database";
        return result;
    }
    
    const auto& versions = it->second;
    result.totalVersions = versions.size();
    
    // Track which version we select and which we skip
    std::optional<size_t> selectedIndex;
    
    for (size_t i = 0; i < versions.size(); ++i) {
        const auto& version = versions[i];
        
        if (version.timestamp <= timestamp) {
            // This version is at or before our query time
            if (selectedIndex.has_value()) {
                // We previously selected a version, now skipping it for this newer one
                result.skippedVersions.push_back(versions[selectedIndex.value()]);
            }
            selectedIndex = i;
        } else {
            // This version is after our query time - skip it
            // (don't add to skippedVersions since it's not relevant for explanation)
            break;
        }
    }
    
    if (selectedIndex.has_value()) {
        result.found = true;
        result.selectedVersion = versions[selectedIndex.value()];
        
        // Build reasoning
        std::stringstream reasoning;
        reasoning << "Selected version at index " << selectedIndex.value() 
                  << " (0-based) out of " << result.totalVersions << " total versions. ";
        reasoning << "This is the most recent version at or before the query timestamp.";
        
        if (!result.skippedVersions.empty()) {
            reasoning << " Skipped " << result.skippedVersions.size() 
                      << " older version(s) that were also valid but superseded.";
        }
        
        // Count versions after query time
        size_t versionsAfter = result.totalVersions - selectedIndex.value() - 1;
        if (versionsAfter > 0) {
            reasoning << " Excluded " << versionsAfter 
                      << " version(s) that occurred after the query timestamp.";
        }
        
        result.reasoning = reasoning.str();
    } else {
        result.reasoning = "No version found at or before the query timestamp. All " 
                          + std::to_string(result.totalVersions) 
                          + " version(s) occurred after the query time.";
    }
    
    return result;
}

std::vector<Version> KVStore::getHistory(const std::string& key) {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    auto it = store.find(key);
    if (it != store.end()) {
        return it->second;
    }
    return std::vector<Version>();
}

bool KVStore::exists(const std::string& key) const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return store.find(key) != store.end();
}

size_t KVStore::size() const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return store.size();
}

std::unordered_map<std::string, std::string> KVStore::getAllData() const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    std::unordered_map<std::string, std::string> result;
    for (const auto& [key, versions] : store) {
        if (!versions.empty()) {
            // Get the latest version
            result[key] = versions.back().value;
        }
    }
    return result;
}

void KVStore::setWalEnabled(bool enabled) {
    walEnabled = enabled;
}

void KVStore::setRetentionPolicy(const RetentionPolicy& policy) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    retentionPolicy = policy;
    
    // Apply new policy to all existing keys
    for (auto& [key, versions] : store) {
        if (!versions.empty()) {
            applyRetention(key);
        }
    }
}

const RetentionPolicy& KVStore::getRetentionPolicy() const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return retentionPolicy;
}

void KVStore::setMaxKeys(size_t maxKeys) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    maxKeys_ = maxKeys;
    evictIfNeeded();
}

size_t KVStore::getMaxKeys() const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return maxKeys_;
}

void KVStore::touchKey(const std::string& key) {
    // Called only from write-locked context
    auto it = lruMap_.find(key);
    if (it != lruMap_.end()) {
        lruOrder_.erase(it->second);
    }
    lruOrder_.push_back(key);
    lruMap_[key] = std::prev(lruOrder_.end());
}

void KVStore::evictIfNeeded() {
    // Called only from write-locked context
    while (store.size() > maxKeys_ && !lruOrder_.empty()) {
        const std::string evictKey = lruOrder_.front();
        lruOrder_.pop_front();
        lruMap_.erase(evictKey);
        store.erase(evictKey);
        spdlog::warn("LRU evict key={} store_size={}", evictKey, store.size());
    }
}

void KVStore::applyRetention(const std::string& key) {
    auto it = store.find(key);
    if (it == store.end() || it->second.empty()) {
        return;
    }
    
    auto& versions = it->second;
    
    switch (retentionPolicy.mode) {
        case RetentionMode::FULL:
            // Keep all versions
            break;
            
        case RetentionMode::LAST_N:
            // Keep only the last N versions
            if (retentionPolicy.count > 0 && versions.size() > static_cast<size_t>(retentionPolicy.count)) {
                // Erase old versions from the beginning
                versions.erase(versions.begin(), 
                              versions.end() - retentionPolicy.count);
            }
            break;
            
        case RetentionMode::LAST_T:
            // Keep only versions within the last T seconds
            if (retentionPolicy.seconds > 0) {
                auto now = std::chrono::system_clock::now();
                auto cutoff = now - std::chrono::seconds(retentionPolicy.seconds);
                
                // Find first version to keep (first one >= cutoff)
                auto firstToKeep = std::find_if(versions.begin(), versions.end(),
                    [cutoff](const Version& v) { return v.timestamp >= cutoff; });
                
                // Erase old versions
                if (firstToKeep != versions.begin()) {
                    versions.erase(versions.begin(), firstToKeep);
                }
            }
            break;
    }
}

// ========== Write Evaluation & Guard Management ==========

WriteEvaluation KVStore::simulateWrite(const std::string& key, const std::string& value) {
    WriteEvaluation evaluation;
    evaluation.key = key;
    evaluation.proposedValue = value;
    evaluation.result = GuardResult::ACCEPT;
    
    // Get applicable guards
    auto applicableGuards = getGuardsForKeyInternal(key);
    
    if (applicableGuards.empty()) {
        evaluation.reason = "No guards defined for this key";
        return evaluation;
    }
    
    // Evaluate each guard
    bool allAccepted = true;

    struct MergedAlternative {
        std::string value;
        double confidenceSum{0.0};
        size_t count{0};
        RiskLevel risk{RiskLevel::LOW};
        std::vector<std::string> reasons;
    };

    auto riskRank = [](RiskLevel level) {
        switch (level) {
            case RiskLevel::LOW: return 0;
            case RiskLevel::MEDIUM: return 1;
            case RiskLevel::HIGH: return 2;
        }
        return 2;
    };

    auto maxRisk = [&](RiskLevel left, RiskLevel right) {
        return riskRank(left) >= riskRank(right) ? left : right;
    };

    std::unordered_map<std::string, MergedAlternative> mergedAlternatives;
    
    for (const auto& guard : applicableGuards) {
        std::string guardReason;
        GuardResult guardResult = guard->evaluate(key, value, guardReason);
        
        if (guardResult == GuardResult::REJECT) {
            evaluation.result = GuardResult::REJECT;
            evaluation.triggeredGuards.push_back(guard->getName());
            evaluation.reason = guardReason;
            // For reject, stop immediately
            return evaluation;
        } else if (guardResult == GuardResult::COUNTER_OFFER) {
            allAccepted = false;
            evaluation.triggeredGuards.push_back(guard->getName());
            
            // Collect alternatives from this guard
            auto guardAlts = guard->generateAlternatives(key, value);
            for (const auto& alt : guardAlts) {
                auto& merged = mergedAlternatives[alt.value];
                if (merged.count == 0) {
                    merged.value = alt.value;
                    merged.risk = alt.risk;
                } else {
                    merged.risk = maxRisk(merged.risk, alt.risk);
                }
                merged.confidenceSum += alt.confidence;
                merged.count += 1;
                merged.reasons.push_back(guard->getName() + ": " + alt.reason);
            }
            
            if (evaluation.reason.empty()) {
                evaluation.reason = guardReason;
            } else {
                evaluation.reason += "; " + guardReason;
            }
        }
    }
    
    if (!allAccepted) {
        evaluation.result = GuardResult::COUNTER_OFFER;
        evaluation.alternatives.clear();
        evaluation.alternatives.reserve(mergedAlternatives.size());

        for (const auto& entry : mergedAlternatives) {
            const auto& merged = entry.second;
            double confidence = merged.count > 0 ? merged.confidenceSum / static_cast<double>(merged.count) : 0.0;

            std::string reason;
            for (size_t i = 0; i < merged.reasons.size(); ++i) {
                if (i > 0) {
                    reason += "; ";
                }
                reason += merged.reasons[i];
            }

            evaluation.alternatives.emplace_back(
                merged.value,
                confidence,
                merged.risk,
                reason.empty() ? "Guard-proposed alternative" : reason
            );
        }

        std::sort(evaluation.alternatives.begin(), evaluation.alternatives.end(),
                  [&](const Alternative& left, const Alternative& right) {
            if (left.confidence != right.confidence) {
                return left.confidence > right.confidence;
            }
            int leftRisk = riskRank(left.risk);
            int rightRisk = riskRank(right.risk);
            if (leftRisk != rightRisk) {
                return leftRisk < rightRisk;
            }
            return left.value < right.value;
        });
    } else {
        evaluation.reason = "All guards passed";
    }
    
    return evaluation;
}

void KVStore::applyDecisionPolicy(WriteEvaluation& evaluation) {
    evaluation.appliedPolicy = decisionPolicy;
    
    // If all guards passed, no policy needed
    if (evaluation.result == GuardResult::ACCEPT) {
        evaluation.policyReasoning = "No policy applied - all guards passed";
        return;
    }
    
    // Apply policy based on current result
    switch (decisionPolicy) {
        case DecisionPolicy::STRICT:
            // STRICT: Convert all COUNTER_OFFER to REJECT
            if (evaluation.result == GuardResult::COUNTER_OFFER) {
                evaluation.result = GuardResult::REJECT;
                evaluation.policyReasoning = "Rejected under STRICT policy due to guard violation";
                evaluation.alternatives.clear();  // No alternatives in strict mode
            } else {
                evaluation.policyReasoning = "Rejected under STRICT policy due to guard violation";
            }
            break;
            
        case DecisionPolicy::DEV_FRIENDLY:
            // DEV_FRIENDLY: Convert REJECT to COUNTER_OFFER if alternatives exist
            if (evaluation.result == GuardResult::REJECT) {
                // Check if we can generate any alternatives
                // For now, keep as REJECT since guards returned it
                evaluation.policyReasoning = "Rejected under DEV_FRIENDLY policy - value cannot be salvaged";
            } else if (evaluation.result == GuardResult::COUNTER_OFFER) {
                evaluation.policyReasoning = "Counter-offer under DEV_FRIENDLY policy - showing alternatives";
            }
            break;
            
        case DecisionPolicy::SAFE_DEFAULT:
            // SAFE_DEFAULT: Analyze risk and decide
            if (evaluation.result == GuardResult::COUNTER_OFFER) {
                // Check if this is a "low-risk" violation
                // For now, we'll consider violations with alternatives as low-risk
                if (evaluation.alternatives.empty()) {
                    // High-risk: no safe alternatives
                    evaluation.result = GuardResult::REJECT;
                    evaluation.policyReasoning = "Rejected under SAFE_DEFAULT policy - no safe alternatives available";
                } else {
                    // Low-risk: alternatives available
                    evaluation.policyReasoning = "Counter-offer under SAFE_DEFAULT policy - safe alternatives available";
                }
            } else if (evaluation.result == GuardResult::REJECT) {
                evaluation.policyReasoning = "Rejected under SAFE_DEFAULT policy - critical violation";
            }
            break;
    }
}

void KVStore::appendAuditEvent(const std::string& key,
                               const std::string& originalValue,
                               const WriteEvaluation& evaluation,
                               const std::optional<std::string>& finalValue) {
    AuditEvent event;
    event.timestamp = std::chrono::system_clock::now();
    event.key = key;
    event.originalValue = originalValue;
    event.guardsFired = evaluation.triggeredGuards;
    event.policyUsed = evaluation.appliedPolicy;
    event.alternativesOffered = evaluation.alternatives;
    event.finalValue = finalValue;

    if (evaluation.result == GuardResult::ACCEPT) {
        event.outcome = "accepted";
    } else if (evaluation.result == GuardResult::COUNTER_OFFER) {
        event.outcome = "rewritten";
    } else {
        event.outcome = "rejected";
    }

    auditLog_.push_back(event);

    if (walEnabled && wal && wal->isEnabled()) {
        std::string guards = joinStrings(event.guardsFired, '|');
        std::vector<std::string> altValues;
        altValues.reserve(event.alternativesOffered.size());
        for (const auto& alt : event.alternativesOffered) {
            altValues.push_back(alt.value);
        }
        std::string alternatives = joinStrings(altValues, '|');
        wal->logAudit(event.key,
                      event.originalValue,
                      event.finalValue.value_or(""),
                      event.outcome,
                      decisionPolicyToString(event.policyUsed),
                      guards,
                      alternatives,
                      event.timestamp);
    }
}

void KVStore::recordLearningObservation(const std::string& key, const std::string& value) {
    auto& stats = learnedStats_[key];
    stats.totalWrites++;
    stats.valueCounts[value]++;

    const size_t maxPrefixLength = std::min<size_t>(12, value.size());
    for (size_t length = 3; length <= maxPrefixLength; ++length) {
        stats.prefixCounts[value.substr(0, length)]++;
    }

    auto numericValue = parseIntegerStrict(value);
    if (!numericValue.has_value()) {
        return;
    }

    stats.numericWrites++;
    stats.numericSamples.push_back(numericValue.value());
    if (!stats.observedMin.has_value() || numericValue.value() < stats.observedMin.value()) {
        stats.observedMin = numericValue.value();
    }
    if (!stats.observedMax.has_value() || numericValue.value() > stats.observedMax.value()) {
        stats.observedMax = numericValue.value();
    }
}

GuardLearningSnapshot KVStore::buildGuardLearningSnapshotUnlocked(const std::string& key,
                                                                 size_t minWrites) const {
    GuardLearningSnapshot snapshot;
    snapshot.key = key;
    snapshot.learningActive = (decisionPolicy == DecisionPolicy::DEV_FRIENDLY);
    snapshot.minWritesThreshold = (minWrites == 0 ? guardLearningMinWrites_ : minWrites);
    snapshot.totalWrites = 0;
    snapshot.numericWrites = 0;
    snapshot.distinctValues = 0;

    auto it = learnedStats_.find(key);
    if (it == learnedStats_.end()) {
        return snapshot;
    }

    const auto& stats = it->second;
    snapshot.totalWrites = stats.totalWrites;
    snapshot.numericWrites = stats.numericWrites;
    snapshot.distinctValues = stats.valueCounts.size();
    snapshot.observedMin = stats.observedMin;
    snapshot.observedMax = stats.observedMax;

    std::vector<std::pair<std::string, size_t>> prefixes;
    prefixes.reserve(stats.prefixCounts.size());
    for (const auto& entry : stats.prefixCounts) {
        if (entry.second >= 2) {
            prefixes.push_back(entry);
        }
    }
    std::sort(prefixes.begin(), prefixes.end(),
              [](const auto& left, const auto& right) {
                  if (left.second != right.second) {
                      return left.second > right.second;
                  }
                  if (left.first.size() != right.first.size()) {
                      return left.first.size() > right.first.size();
                  }
                  return left.first < right.first;
              });
    for (const auto& entry : prefixes) {
        if (snapshot.commonPrefixes.size() == 5) {
            break;
        }
        if (entry.second * 100 < std::max<size_t>(1, stats.totalWrites) * 60) {
            continue;
        }
        snapshot.commonPrefixes.push_back(entry);
    }

    if (stats.totalWrites < snapshot.minWritesThreshold) {
        return snapshot;
    }

    if (stats.numericWrites >= snapshot.minWritesThreshold && !stats.numericSamples.empty()) {
        const size_t coverageCount = std::max<size_t>(
            1, static_cast<size_t>(std::ceil(static_cast<double>(stats.numericSamples.size()) * 0.95)));
        auto [windowMin, windowMax] = smallestCoveringWindow(stats.numericSamples, coverageCount);

        GuardSuggestion suggestion;
        suggestion.type = "RANGE_INT";
        suggestion.confidence = static_cast<double>(coverageCount) /
                                static_cast<double>(std::max<size_t>(1, stats.totalWrites));
        suggestion.supportingWrites = coverageCount;
        suggestion.suggestedMin = windowMin;
        suggestion.suggestedMax = windowMax;
        suggestion.recommendation = formatPercentage(coverageCount, stats.totalWrites) +
            "% of values were between " + std::to_string(windowMin) + " and " +
            std::to_string(windowMax) + ", consider RANGE " +
            std::to_string(windowMin) + " " + std::to_string(windowMax);
        snapshot.suggestions.push_back(std::move(suggestion));
    }

    if (!stats.valueCounts.empty() && stats.valueCounts.size() <= 5) {
        GuardSuggestion suggestion;
        suggestion.type = "ENUM";
        suggestion.confidence = 1.0;
        suggestion.supportingWrites = stats.totalWrites;

        std::vector<std::pair<std::string, size_t>> orderedValues(stats.valueCounts.begin(), stats.valueCounts.end());
        std::sort(orderedValues.begin(), orderedValues.end(),
                  [](const auto& left, const auto& right) {
                      if (left.second != right.second) {
                          return left.second > right.second;
                      }
                      return left.first < right.first;
                  });

        std::vector<std::string> enumValues;
        enumValues.reserve(orderedValues.size());
        for (const auto& entry : orderedValues) {
            enumValues.push_back(entry.first);
        }
        suggestion.enumValues = enumValues;
        suggestion.recommendation = "Observed " + std::to_string(stats.valueCounts.size()) +
            " distinct values across " + std::to_string(stats.totalWrites) +
            " writes, consider ENUM {" + joinStrings(enumValues, ',') + "}";
        snapshot.suggestions.push_back(std::move(suggestion));
    }

    if (!snapshot.commonPrefixes.empty()) {
        const auto& bestPrefix = snapshot.commonPrefixes.front();
        GuardSuggestion suggestion;
        suggestion.type = "PATTERN";
        suggestion.confidence = static_cast<double>(bestPrefix.second) /
                                static_cast<double>(std::max<size_t>(1, stats.totalWrites));
        suggestion.supportingWrites = bestPrefix.second;
        suggestion.prefix = bestPrefix.first;
        suggestion.recommendation = formatPercentage(bestPrefix.second, stats.totalWrites) +
            "% of values started with '" + bestPrefix.first +
            "', consider PATTERN ^" + bestPrefix.first + ".*";
        snapshot.suggestions.push_back(std::move(suggestion));
    }

    std::sort(snapshot.suggestions.begin(), snapshot.suggestions.end(),
              [](const GuardSuggestion& left, const GuardSuggestion& right) {
                  if (left.confidence != right.confidence) {
                      return left.confidence > right.confidence;
                  }
                  return left.type < right.type;
              });

    return snapshot;
}

WriteEvaluation KVStore::proposeSet(const std::string& key, const std::string& value) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    // Simulate without mutating state
    auto evaluation = simulateWrite(key, value);
    
    // Apply decision policy to the evaluation result
    applyDecisionPolicy(evaluation);

    std::optional<std::string> finalValue;
    if (evaluation.result == GuardResult::ACCEPT) {
        finalValue = value;
    } else if (evaluation.result == GuardResult::COUNTER_OFFER && !evaluation.alternatives.empty()) {
        finalValue = evaluation.alternatives.front().value;
    }
    appendAuditEvent(key, value, evaluation, finalValue);
    
    return evaluation;
}

Status KVStore::commitSet(const std::string& key, const std::string& value) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    // Direct commit (bypasses guards - use for forced writes)
    return setInternal(key, value);
}

NegotiatedWriteResult KVStore::safeSet(const std::string& key, const std::string& value) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);

    NegotiatedWriteResult result;
    result.evaluation = simulateWrite(key, value);
    applyDecisionPolicy(result.evaluation);

    std::string chosenValue;
    if (result.evaluation.result == GuardResult::ACCEPT) {
        chosenValue = value;
    } else if (result.evaluation.result == GuardResult::COUNTER_OFFER &&
               !result.evaluation.alternatives.empty()) {
        // Alternatives are already ordered by each guard's recommendation quality.
        chosenValue = result.evaluation.alternatives.front().value;
    } else {
        appendAuditEvent(key, value, result.evaluation, std::nullopt);
        return result;
    }

    appendAuditEvent(key, value, result.evaluation, chosenValue);

    if (setInternal(key, chosenValue) == Status::OK) {
        result.committed = true;
        result.storedValue = chosenValue;
    }

    return result;
}

BatchPlan KVStore::planBatch(const std::vector<std::pair<std::string, std::string>>& writes) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);

    BatchPlan plan;
    plan.canCommit = true;

    std::unordered_map<std::string, std::string> currentValues;
    currentValues.reserve(store.size());
    for (const auto& entry : store) {
        if (!entry.second.empty()) {
            currentValues[entry.first] = entry.second.back().value;
        }
    }

    auto storeProvider = [this](const std::string& targetKey) -> std::optional<std::string> {
        auto it = store.find(targetKey);
        if (it != store.end() && !it->second.empty()) {
            return it->second.back().value;
        }
        return std::nullopt;
    };

    auto batchProvider = [&currentValues](const std::string& targetKey) -> std::optional<std::string> {
        auto it = currentValues.find(targetKey);
        if (it != currentValues.end()) {
            return it->second;
        }
        return std::nullopt;
    };

    for (const auto& guard : guards) {
        guard->setValueProvider(batchProvider);
    }

    plan.items.reserve(writes.size());
    plan.finalWrites.reserve(writes.size());

    for (const auto& write : writes) {
        PlannedWrite item;
        item.key = write.first;
        item.proposedValue = write.second;
        item.rewritten = false;
        item.hasFinalValue = false;

        item.evaluation = simulateWrite(item.key, item.proposedValue);
        applyDecisionPolicy(item.evaluation);

        std::string finalValue;
        if (item.evaluation.result == GuardResult::ACCEPT) {
            finalValue = item.proposedValue;
            item.hasFinalValue = true;
        } else if (item.evaluation.result == GuardResult::COUNTER_OFFER &&
                   !item.evaluation.alternatives.empty()) {
            finalValue = item.evaluation.alternatives.front().value;
            item.rewritten = finalValue != item.proposedValue;
            item.hasFinalValue = true;
        } else {
            plan.canCommit = false;
        }

        appendAuditEvent(item.key,
                         item.proposedValue,
                         item.evaluation,
                         item.hasFinalValue ? std::optional<std::string>(finalValue) : std::nullopt);

        if (item.hasFinalValue) {
            item.finalValue = finalValue;
            plan.finalWrites.emplace_back(item.key, finalValue);
            currentValues[item.key] = finalValue;
        } else {
            item.finalValue.clear();
        }

        plan.items.push_back(std::move(item));
    }

    for (const auto& guard : guards) {
        guard->setValueProvider(storeProvider);
    }

    return plan;
}

Status KVStore::commitBatch(const std::vector<std::pair<std::string, std::string>>& writes) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    Status status = Status::OK;

    for (const auto& write : writes) {
        if (setInternal(write.first, write.second) != Status::OK) {
            status = Status::ERROR;
            break;
        }
    }

    return status;
}

void KVStore::addGuard(std::shared_ptr<Guard> guard) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    guard->setValueProvider([this](const std::string& targetKey) -> std::optional<std::string> {
        auto it = store.find(targetKey);
        if (it != store.end() && !it->second.empty()) {
            return it->second.back().value;
        }
        return std::nullopt;
    });
    guards.push_back(guard);
}

bool KVStore::hasGuard(const std::string& name) const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return std::find_if(guards.begin(), guards.end(),
        [&name](const std::shared_ptr<Guard>& g) { 
            return g->getName() == name; 
        }) != guards.end();
}

bool KVStore::removeGuard(const std::string& name) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    auto it = std::find_if(guards.begin(), guards.end(),
        [&name](const std::shared_ptr<Guard>& g) { return g->getName() == name; });
    
    if (it != guards.end()) {
        guards.erase(it);
        return true;
    }
    return false;
}

const std::vector<std::shared_ptr<Guard>>& KVStore::getGuards() const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return guards;
}

std::vector<std::shared_ptr<Guard>> KVStore::getGuardsForKey(const std::string& key) const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return getGuardsForKeyInternal(key);
}

std::vector<std::shared_ptr<Guard>> KVStore::getGuardsForKeyInternal(const std::string& key) const {
    std::vector<std::shared_ptr<Guard>> applicable;
    
    for (const auto& guard : guards) {
        if (guard->isEnabled() && guard->appliesTo(key)) {
            applicable.push_back(guard);
        }
    }
    
    return applicable;
}

void KVStore::setDecisionPolicy(DecisionPolicy policy) {
    // Thread safety: reader/writer lock
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    decisionPolicy = policy;
    
    // Log policy change to WAL
    if (walEnabled && wal && wal->isEnabled()) {
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
        wal->logPolicy(policyName);
    }
}

DecisionPolicy KVStore::getDecisionPolicy() const {
    // Thread safety: reader/writer lock
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return decisionPolicy;
}

void KVStore::setGuardLearningMinWrites(size_t minWrites) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    guardLearningMinWrites_ = std::max<size_t>(1, minWrites);
}

size_t KVStore::getGuardLearningMinWrites() const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return guardLearningMinWrites_;
}

GuardLearningSnapshot KVStore::getGuardSuggestions(const std::string& key, size_t minWrites) const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    return buildGuardLearningSnapshotUnlocked(key, minWrites);
}

std::vector<AuditEvent> KVStore::getAuditEvents(
    const std::string& key,
    const std::optional<std::chrono::system_clock::time_point>& since) const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    std::vector<AuditEvent> events;

    for (const auto& event : auditLog_) {
        if (!key.empty() && event.key != key) {
            continue;
        }
        if (since.has_value() && event.timestamp < since.value()) {
            continue;
        }
        events.push_back(event);
    }

    return events;
}

SimulationResult KVStore::simulateAtTime(const std::string& key,
                                         const std::string& value,
                                         std::chrono::system_clock::time_point timestamp) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    SimulationResult result;
    result.key = key;
    result.timestamp = timestamp;
    result.proposedValue = value;

    std::vector<std::string> historicalGuards;
    DecisionPolicy historicalPolicy = decisionPolicy;
    bool policyFound = false;

    for (const auto& event : auditLog_) {
        if (event.timestamp > timestamp) {
            continue;
        }
        if (!event.guardsFired.empty()) {
            for (const auto& guardName : event.guardsFired) {
                if (std::find(historicalGuards.begin(), historicalGuards.end(), guardName) == historicalGuards.end()) {
                    historicalGuards.push_back(guardName);
                }
            }
        }
        historicalPolicy = event.policyUsed;
        policyFound = true;
    }

    if (!policyFound) {
        historicalPolicy = decisionPolicy;
    }

    auto getValueAt = [&](const std::string& targetKey, std::chrono::system_clock::time_point ts)
        -> std::optional<std::string> {
        auto it = store.find(targetKey);
        if (it == store.end() || it->second.empty()) {
            return std::nullopt;
        }

        const auto& versions = it->second;
        auto upper = std::upper_bound(
            versions.begin(),
            versions.end(),
            ts,
            [](const auto& timepoint, const Version& version) {
                return timepoint < version.timestamp;
            }
        );

        if (upper == versions.begin()) {
            return std::nullopt;
        }

        return std::prev(upper)->value;
    };

    auto storeProvider = [this](const std::string& targetKey) -> std::optional<std::string> {
        auto it = store.find(targetKey);
        if (it != store.end() && !it->second.empty()) {
            return it->second.back().value;
        }
        return std::nullopt;
    };

    auto timeProvider = [&](const std::string& targetKey) -> std::optional<std::string> {
        return getValueAt(targetKey, timestamp);
    };

    std::vector<std::shared_ptr<Guard>> guardsToUse;
    for (const auto& guard : guards) {
        if (!guard->isEnabled() || !guard->appliesTo(key)) {
            continue;
        }
        if (historicalGuards.empty()) {
            continue;
        }
        if (std::find(historicalGuards.begin(), historicalGuards.end(), guard->getName()) == historicalGuards.end()) {
            continue;
        }
        guardsToUse.push_back(guard);
    }

    result.guardsConsidered.reserve(guardsToUse.size());
    for (const auto& guard : guardsToUse) {
        result.guardsConsidered.push_back(guard->getName());
        guard->setValueProvider(timeProvider);
    }

    WriteEvaluation evaluation;
    evaluation.key = key;
    evaluation.proposedValue = value;
    evaluation.result = GuardResult::ACCEPT;

    if (guardsToUse.empty()) {
        evaluation.reason = "No historical guards available for timestamp";
    } else {
        bool allAccepted = true;

        struct MergedAlternative {
            std::string altValue;
            double confidenceSum{0.0};
            size_t count{0};
            RiskLevel risk{RiskLevel::LOW};
            std::vector<std::string> reasons;
        };

        auto riskRank = [](RiskLevel level) {
            switch (level) {
                case RiskLevel::LOW: return 0;
                case RiskLevel::MEDIUM: return 1;
                case RiskLevel::HIGH: return 2;
            }
            return 2;
        };

        auto maxRisk = [&](RiskLevel left, RiskLevel right) {
            return riskRank(left) >= riskRank(right) ? left : right;
        };

        std::unordered_map<std::string, MergedAlternative> mergedAlternatives;

        for (const auto& guard : guardsToUse) {
            std::string guardReason;
            GuardResult guardResult = guard->evaluate(key, value, guardReason);

            if (guardResult == GuardResult::REJECT) {
                evaluation.result = GuardResult::REJECT;
                evaluation.triggeredGuards.push_back(guard->getName());
                evaluation.reason = guardReason;
                break;
            } else if (guardResult == GuardResult::COUNTER_OFFER) {
                allAccepted = false;
                evaluation.triggeredGuards.push_back(guard->getName());
                auto guardAlts = guard->generateAlternatives(key, value);
                for (const auto& alt : guardAlts) {
                    auto& merged = mergedAlternatives[alt.value];
                    if (merged.count == 0) {
                        merged.altValue = alt.value;
                        merged.risk = alt.risk;
                    } else {
                        merged.risk = maxRisk(merged.risk, alt.risk);
                    }
                    merged.confidenceSum += alt.confidence;
                    merged.count += 1;
                    merged.reasons.push_back(guard->getName() + ": " + alt.reason);
                }

                if (evaluation.reason.empty()) {
                    evaluation.reason = guardReason;
                } else {
                    evaluation.reason += "; " + guardReason;
                }
            }
        }

        if (evaluation.result != GuardResult::REJECT) {
            if (!allAccepted) {
                evaluation.result = GuardResult::COUNTER_OFFER;
                evaluation.alternatives.clear();
                evaluation.alternatives.reserve(mergedAlternatives.size());
                for (const auto& entry : mergedAlternatives) {
                    const auto& merged = entry.second;
                    double confidence = merged.count > 0 ? merged.confidenceSum / static_cast<double>(merged.count) : 0.0;
                    std::string reason;
                    for (size_t i = 0; i < merged.reasons.size(); ++i) {
                        if (i > 0) {
                            reason += "; ";
                        }
                        reason += merged.reasons[i];
                    }
                    evaluation.alternatives.emplace_back(
                        merged.altValue,
                        confidence,
                        merged.risk,
                        reason.empty() ? "Guard-proposed alternative" : reason
                    );
                }

                std::sort(evaluation.alternatives.begin(), evaluation.alternatives.end(),
                          [&](const Alternative& left, const Alternative& right) {
                    if (left.confidence != right.confidence) {
                        return left.confidence > right.confidence;
                    }
                    int leftRisk = riskRank(left.risk);
                    int rightRisk = riskRank(right.risk);
                    if (leftRisk != rightRisk) {
                        return leftRisk < rightRisk;
                    }
                    return left.value < right.value;
                });
            } else {
                evaluation.reason = "All guards passed";
            }
        }
    }

    auto priorPolicy = decisionPolicy;
    decisionPolicy = historicalPolicy;
    applyDecisionPolicy(evaluation);
    decisionPolicy = priorPolicy;

    for (const auto& guard : guardsToUse) {
        guard->setValueProvider(storeProvider);
    }

    result.evaluation = evaluation;
    result.policyUsed = historicalPolicy;
    return result;
}
