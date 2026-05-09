#ifndef GUARD_H
#define GUARD_H

#include <string>
#include <vector>
#include <optional>
#include <functional>
#include <memory>

// Decision policy for handling guard violations
enum class DecisionPolicy {
    DEV_FRIENDLY,   // Always negotiate when possible, never reject
    SAFE_DEFAULT,   // Negotiate low-risk violations, reject high-risk
    STRICT          // Reject all guard violations
};

// Result of guard evaluation
enum class GuardResult {
    ACCEPT,
    REJECT,
    COUNTER_OFFER
};

// Alternative value with explanation
struct Alternative {
    std::string value;
    std::string explanation;
    
    Alternative(const std::string& v, const std::string& e)
        : value(v), explanation(e) {}
};

// Result of write evaluation
struct WriteEvaluation {
    GuardResult result;
    std::string key;
    std::string proposedValue;
    std::string reason;
    std::vector<Alternative> alternatives;
    std::vector<std::string> triggeredGuards;  // Names of guards that triggered
    DecisionPolicy appliedPolicy;
    std::string policyReasoning;  // Why the policy made this decision
    
    WriteEvaluation() : result(GuardResult::ACCEPT), appliedPolicy(DecisionPolicy::SAFE_DEFAULT) {}
};

// Result of a server-owned negotiate-and-commit write
struct NegotiatedWriteResult {
    WriteEvaluation evaluation;
    bool committed;
    std::string storedValue;

    NegotiatedWriteResult() : committed(false) {}
};

// Guard constraint types
enum class GuardType {
    RANGE_INT,      // Integer range check
    RANGE_DOUBLE,   // Double range check
    PATTERN,        // Regex pattern match
    ENUM_VALUES,    // Must be one of predefined values
    MIN_LENGTH,     // Minimum string length
    MAX_LENGTH,     // Maximum string length
    CUSTOM          // Custom validation function
};

// Base guard constraint
class Guard {
protected:
    std::string name;
    std::string key;  // Key pattern this guard applies to (supports wildcards)
    bool enabled;
    
public:
    Guard(const std::string& n, const std::string& k)
        : name(n), key(k), enabled(true) {}
    
    virtual ~Guard() = default;
    
    // Evaluate if the proposed value is acceptable
    virtual GuardResult evaluate(const std::string& proposedValue, std::string& reason) const = 0;
    
    // Generate safe alternatives if rejected
    virtual std::vector<Alternative> generateAlternatives(const std::string& proposedValue) const = 0;
    
    // Check if this guard applies to a given key
    virtual bool appliesTo(const std::string& targetKey) const;
    
    std::string getName() const { return name; }
    std::string getKeyPattern() const { return key; }
    bool isEnabled() const { return enabled; }
    void setEnabled(bool e) { enabled = e; }
    
    virtual std::string describe() const = 0;
};

// Integer range guard
class RangeIntGuard : public Guard {
private:
    int minValue;
    int maxValue;
    
public:
    RangeIntGuard(const std::string& name, const std::string& key, int min, int max)
        : Guard(name, key), minValue(min), maxValue(max) {}
    
    GuardResult evaluate(const std::string& proposedValue, std::string& reason) const override;
    std::vector<Alternative> generateAlternatives(const std::string& proposedValue) const override;
    std::string describe() const override;
    int getMinValue() const { return minValue; }
    int getMaxValue() const { return maxValue; }
};

// Enum values guard
class EnumGuard : public Guard {
private:
    std::vector<std::string> allowedValues;
    
public:
    EnumGuard(const std::string& name, const std::string& key, 
              const std::vector<std::string>& values)
        : Guard(name, key), allowedValues(values) {}
    
    GuardResult evaluate(const std::string& proposedValue, std::string& reason) const override;
    std::vector<Alternative> generateAlternatives(const std::string& proposedValue) const override;
    std::string describe() const override;
    const std::vector<std::string>& getAllowedValues() const { return allowedValues; }
};

// Length guard
class LengthGuard : public Guard {
private:
    size_t minLength;
    size_t maxLength;
    
public:
    LengthGuard(const std::string& name, const std::string& key, 
                size_t min, size_t max)
        : Guard(name, key), minLength(min), maxLength(max) {}
    
    GuardResult evaluate(const std::string& proposedValue, std::string& reason) const override;
    std::vector<Alternative> generateAlternatives(const std::string& proposedValue) const override;
    std::string describe() const override;
    size_t getMinLength() const { return minLength; }
    size_t getMaxLength() const { return maxLength; }
};

#endif // GUARD_H
