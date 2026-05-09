#ifndef KVSTORE_H
#define KVSTORE_H

#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <memory>
#include <chrono>
#include <list>
#include <shared_mutex>
#include <deque>
#include <utility>
#include "status.h"
#include "wal.h"
#include "guard.h"

// Represents a versioned value with timestamp
struct Version {
    std::chrono::system_clock::time_point timestamp;
    std::string value;
    
    Version(const std::string& val) 
        : timestamp(std::chrono::system_clock::now()), value(val) {}
    
    Version(std::chrono::system_clock::time_point ts, const std::string& val)
        : timestamp(ts), value(val) {}
};

// Retention policy modes
enum class RetentionMode {
    FULL,    // Keep all versions
    LAST_N,  // Keep only the last N versions
    LAST_T   // Keep only versions within the last T seconds
};

// Retention policy configuration
struct RetentionPolicy {
    RetentionMode mode;
    int count;        // For LAST_N mode: number of versions to keep
    int seconds;      // For LAST_T mode: time window in seconds
    
    // Default: keep all versions
    RetentionPolicy() : mode(RetentionMode::FULL), count(0), seconds(0) {}
    
    RetentionPolicy(RetentionMode m, int val) 
        : mode(m), count(m == RetentionMode::LAST_N ? val : 0), 
          seconds(m == RetentionMode::LAST_T ? val : 0) {}
};

// Explain result for temporal queries
struct ExplainResult {
    bool found;
    std::string key;
    std::chrono::system_clock::time_point queryTimestamp;
    std::optional<Version> selectedVersion;
    std::string reasoning;
    std::vector<Version> skippedVersions;
    size_t totalVersions;
};

struct PlannedWrite {
    std::string key;
    std::string proposedValue;
    std::string finalValue;
    bool hasFinalValue;
    bool rewritten;
    WriteEvaluation evaluation;
};

struct BatchPlan {
    std::vector<PlannedWrite> items;
    bool canCommit;
    std::vector<std::pair<std::string, std::string>> finalWrites;
};

struct DiffResult {
    std::string key;
    std::chrono::system_clock::time_point from;
    std::chrono::system_clock::time_point to;
    bool changed;
    std::optional<std::string> oldValue;
    std::optional<std::string> newValue;
    bool hasEvaluation;
    WriteEvaluation evaluation;
};

struct AuditEvent {
    std::chrono::system_clock::time_point timestamp;
    std::string key;
    std::string originalValue;
    std::vector<std::string> guardsFired;
    DecisionPolicy policyUsed;
    std::vector<Alternative> alternativesOffered;
    std::optional<std::string> finalValue;
    std::string outcome;
};

struct RollbackResult {
    std::string key;
    std::chrono::system_clock::time_point targetTimestamp;
    bool found;
    std::optional<std::string> rollbackValue;
    bool committed;
    std::optional<std::string> storedValue;
    bool hasSuggestedValue;
    std::optional<std::string> suggestedValue;
    WriteEvaluation evaluation;
};

class KVStore {
private:
    std::unordered_map<std::string, std::vector<Version>> store;
    std::shared_ptr<WAL> wal;
    bool walEnabled;
    RetentionPolicy retentionPolicy;
    std::vector<std::shared_ptr<Guard>> guards;  // Active guard constraints
    DecisionPolicy decisionPolicy;  // Active decision policy for guard violations
    mutable std::shared_mutex rwMutex_;
    std::deque<AuditEvent> auditLog_;

    // Internal set implementation for already-locked callers
    Status setInternal(const std::string& key, const std::string& value);

    // Internal helper for already-locked callers
    std::vector<std::shared_ptr<Guard>> getGuardsForKeyInternal(const std::string& key) const;

    // LRU eviction
    size_t maxKeys_{100000};
    std::list<std::string> lruOrder_;
    std::unordered_map<std::string, std::list<std::string>::iterator> lruMap_;
    void touchKey(const std::string& key);
    void evictIfNeeded();
    
    // Apply retention policy to a key's versions
    void applyRetention(const std::string& key);
    
    // Simulate write on copy without mutating real state
    WriteEvaluation simulateWrite(const std::string& key, const std::string& value);
    
    // Apply decision policy to guard evaluation results
    void applyDecisionPolicy(WriteEvaluation& evaluation);

    // Append an audit event for a negotiated write
    void appendAuditEvent(const std::string& key,
                          const std::string& originalValue,
                          const WriteEvaluation& evaluation,
                          const std::optional<std::string>& finalValue);

public:
    // Constructor with optional WAL
    explicit KVStore(std::shared_ptr<WAL> walPtr = nullptr);
    
    // Set a key-value pair (creates version with current timestamp)
    Status set(const std::string& key, const std::string& value);
    
    // Set a key-value pair with specific timestamp (for replay)
    Status setAtTime(const std::string& key, const std::string& value,
                     std::chrono::system_clock::time_point timestamp);
    
    // Get a value by key
    std::optional<std::string> get(const std::string& key);
    
    // Delete a key
    Status del(const std::string& key);
    
    // Get value at or before a specific timestamp
    std::optional<std::string> getAtTime(const std::string& key, 
                                          std::chrono::system_clock::time_point timestamp);

    // Diff a key between two timestamps with guard evaluation
    DiffResult diffAtTime(const std::string& key,
                          std::chrono::system_clock::time_point t1,
                          std::chrono::system_clock::time_point t2);

    // Roll back a key to a timestamp with guard evaluation
    RollbackResult rollbackToTime(const std::string& key,
                                  std::chrono::system_clock::time_point timestamp);

    // Retrieve audit events for a key since an optional timestamp
    std::vector<AuditEvent> getAuditEvents(const std::string& key,
                                           const std::optional<std::chrono::system_clock::time_point>& since) const;
    
    // Explain temporal query - shows reasoning for version selection
    ExplainResult explainGetAtTime(const std::string& key,
                                   std::chrono::system_clock::time_point timestamp);
    
    // Get all versions of a key
    std::vector<Version> getHistory(const std::string& key);
    
    // Check if key exists
    bool exists(const std::string& key) const;
    
    // Get the number of keys
    size_t size() const;
    
    // Get all data (for snapshot creation) - returns latest version of each key
    std::unordered_map<std::string, std::string> getAllData() const;
    
    // Disable/enable WAL temporarily (for replay)
    void setWalEnabled(bool enabled);
    
    // Set retention policy
    void setRetentionPolicy(const RetentionPolicy& policy);
    
    // Get current retention policy
    const RetentionPolicy& getRetentionPolicy() const;

    // Set/get maximum number of keys before LRU eviction
    void setMaxKeys(size_t maxKeys);
    size_t getMaxKeys() const;
    
    // ========== Write Evaluation & Guard Management ==========
    
    // Propose a write - evaluates guards without committing
    WriteEvaluation proposeSet(const std::string& key, const std::string& value);
    
    // Commit a write (after proposal accepted or user override)
    Status commitSet(const std::string& key, const std::string& value);

    // Atomically negotiate and commit a write using the best safe alternative
    NegotiatedWriteResult safeSet(const std::string& key, const std::string& value);

    // Plan a batch of writes without committing
    BatchPlan planBatch(const std::vector<std::pair<std::string, std::string>>& writes);

    // Commit a batch of writes atomically
    Status commitBatch(const std::vector<std::pair<std::string, std::string>>& writes);
    
    // Add a guard constraint
    void addGuard(std::shared_ptr<Guard> guard);
    
    // Check if a guard with the given name exists
    bool hasGuard(const std::string& name) const;
    
    // Remove a guard by name
    bool removeGuard(const std::string& name);
    
    // Get all guards
    const std::vector<std::shared_ptr<Guard>>& getGuards() const;
    
    // Get guards that apply to a specific key
    std::vector<std::shared_ptr<Guard>> getGuardsForKey(const std::string& key) const;
    
    // Set decision policy
    void setDecisionPolicy(DecisionPolicy policy);
    
    // Get current decision policy
    DecisionPolicy getDecisionPolicy() const;
};

#endif // KVSTORE_H
