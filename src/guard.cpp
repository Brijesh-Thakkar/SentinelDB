#include "guard.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <cmath>

namespace {
double clampConfidence(double value) {
    if (value < 0.0) return 0.0;
    if (value > 1.0) return 1.0;
    return value;
}

bool tryParseDouble(const std::string& text, double& out) {
    try {
        size_t pos = 0;
        out = std::stod(text, &pos);
        return pos == text.size();
    } catch (...) {
        return false;
    }
}

std::string formatDouble(double value) {
    std::ostringstream oss;
    oss.setf(std::ios::fixed);
    oss << std::setprecision(6) << value;
    std::string text = oss.str();
    auto trimPos = text.find_last_not_of('0');
    if (trimPos != std::string::npos && trimPos + 1 < text.size()) {
        if (text[trimPos] == '.') {
            trimPos--;
        }
        text.erase(trimPos + 1);
    }
    return text;
}
}

// Helper function for wildcard matching
bool Guard::appliesTo(const std::string& targetKey) const {
    // Simple wildcard matching: * matches any characters
    if (key == "*") return true;
    if (key == targetKey) return true;
    
    // Check for prefix wildcard: "price*" matches "price", "price_usd", etc.
    if (key.back() == '*') {
        std::string prefix = key.substr(0, key.length() - 1);
        return targetKey.substr(0, prefix.length()) == prefix;
    }
    
    return false;
}

// ============= RangeIntGuard Implementation =============

GuardResult RangeIntGuard::evaluate(const std::string&,
                                    const std::string& proposedValue,
                                    std::string& reason) const {
    try {
        int value = std::stoi(proposedValue);
        
        if (value >= minValue && value <= maxValue) {
            reason = "Value within acceptable range [" + std::to_string(minValue) + 
                     ", " + std::to_string(maxValue) + "]";
            return GuardResult::ACCEPT;
        } else {
            reason = "Value " + std::to_string(value) + " outside acceptable range [" + 
                     std::to_string(minValue) + ", " + std::to_string(maxValue) + "]";
            return GuardResult::COUNTER_OFFER;
        }
    } catch (...) {
        reason = "Value is not a valid integer";
        return GuardResult::REJECT;
    }
}

std::vector<Alternative> RangeIntGuard::generateAlternatives(const std::string&,
                                                             const std::string& proposedValue) const {
    std::vector<Alternative> alternatives;
    
    try {
        int value = std::stoi(proposedValue);
        
        if (value < minValue) {
            alternatives.emplace_back(
                std::to_string(minValue),
                clampConfidence(0.95),
                RiskLevel::LOW,
                "Minimum allowed value (proposed " + std::to_string(value) + " is too low)"
            );
            
            // Suggest midpoint if there's room
            if (maxValue > minValue) {
                int midpoint = minValue + (maxValue - minValue) / 4;
                alternatives.emplace_back(
                    std::to_string(midpoint),
                    clampConfidence(0.8),
                    RiskLevel::MEDIUM,
                    "Conservative value within range"
                );
            }
        } else if (value > maxValue) {
            alternatives.emplace_back(
                std::to_string(maxValue),
                clampConfidence(0.95),
                RiskLevel::LOW,
                "Maximum allowed value (proposed " + std::to_string(value) + " is too high)"
            );
            
            // Suggest midpoint if there's room
            if (maxValue > minValue) {
                int midpoint = maxValue - (maxValue - minValue) / 4;
                alternatives.emplace_back(
                    std::to_string(midpoint),
                    clampConfidence(0.8),
                    RiskLevel::MEDIUM,
                    "Conservative value within range"
                );
            }
        }
    } catch (...) {
        // Invalid integer, suggest valid examples
        alternatives.emplace_back(
            std::to_string(minValue),
            clampConfidence(0.85),
            RiskLevel::LOW,
            "Minimum allowed value"
        );
        alternatives.emplace_back(
            std::to_string((minValue + maxValue) / 2),
            clampConfidence(0.7),
            RiskLevel::MEDIUM,
            "Midpoint value"
        );
        alternatives.emplace_back(
            std::to_string(maxValue),
            clampConfidence(0.85),
            RiskLevel::LOW,
            "Maximum allowed value"
        );
    }
    
    return alternatives;
}

std::string RangeIntGuard::describe() const {
    return "Integer range: [" + std::to_string(minValue) + ", " + 
           std::to_string(maxValue) + "]";
}

// ============= EnumGuard Implementation =============

GuardResult EnumGuard::evaluate(const std::string&,
                                const std::string& proposedValue,
                                std::string& reason) const {
    auto it = std::find(allowedValues.begin(), allowedValues.end(), proposedValue);
    
    if (it != allowedValues.end()) {
        reason = "Value is in allowed set";
        return GuardResult::ACCEPT;
    } else {
        std::stringstream ss;
        ss << "Value '" << proposedValue << "' not in allowed set: {";
        for (size_t i = 0; i < allowedValues.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << "'" << allowedValues[i] << "'";
        }
        ss << "}";
        reason = ss.str();
        return GuardResult::COUNTER_OFFER;
    }
}

std::vector<Alternative> EnumGuard::generateAlternatives(const std::string&,
                                                         const std::string& proposedValue) const {
    std::vector<Alternative> alternatives;
    
    // Suggest values based on similarity (simple case-insensitive match)
    std::string lowerProposed = proposedValue;
    std::transform(lowerProposed.begin(), lowerProposed.end(), 
                   lowerProposed.begin(), ::tolower);
    
    // First, add exact case-insensitive matches
    for (const auto& allowed : allowedValues) {
        std::string lowerAllowed = allowed;
        std::transform(lowerAllowed.begin(), lowerAllowed.end(), 
                       lowerAllowed.begin(), ::tolower);
        
        if (lowerAllowed == lowerProposed) {
            alternatives.emplace_back(
                allowed,
                clampConfidence(0.95),
                RiskLevel::LOW,
                "Case-corrected version of proposed value"
            );
        }
    }
    
    // Then add partial matches
    for (const auto& allowed : allowedValues) {
        std::string lowerAllowed = allowed;
        std::transform(lowerAllowed.begin(), lowerAllowed.end(), 
                       lowerAllowed.begin(), ::tolower);
        
        if (lowerAllowed.find(lowerProposed) != std::string::npos ||
            lowerProposed.find(lowerAllowed) != std::string::npos) {
            
            // Avoid duplicates
            bool alreadyAdded = false;
            for (const auto& alt : alternatives) {
                if (alt.value == allowed) {
                    alreadyAdded = true;
                    break;
                }
            }
            
            if (!alreadyAdded) {
                alternatives.emplace_back(
                    allowed,
                    clampConfidence(0.7),
                    RiskLevel::MEDIUM,
                    "Similar to proposed value"
                );
            }
        }
    }
    
    // If no matches found, suggest first few allowed values
    if (alternatives.empty()) {
        size_t suggestCount = std::min(size_t(3), allowedValues.size());
        for (size_t i = 0; i < suggestCount; ++i) {
            alternatives.emplace_back(
                allowedValues[i],
                clampConfidence(0.6),
                RiskLevel::MEDIUM,
                "Allowed value"
            );
        }
    }
    
    return alternatives;
}

std::string EnumGuard::describe() const {
    std::stringstream ss;
    ss << "Allowed values: {";
    for (size_t i = 0; i < allowedValues.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << "'" << allowedValues[i] << "'";
    }
    ss << "}";
    return ss.str();
}

// ============= LengthGuard Implementation =============

GuardResult LengthGuard::evaluate(const std::string&,
                                  const std::string& proposedValue,
                                  std::string& reason) const {
    size_t len = proposedValue.length();
    
    if (len >= minLength && len <= maxLength) {
        reason = "Length " + std::to_string(len) + " within acceptable range [" + 
                 std::to_string(minLength) + ", " + std::to_string(maxLength) + "]";
        return GuardResult::ACCEPT;
    } else {
        reason = "Length " + std::to_string(len) + " outside acceptable range [" + 
                 std::to_string(minLength) + ", " + std::to_string(maxLength) + "]";
        return GuardResult::COUNTER_OFFER;
    }
}

std::vector<Alternative> LengthGuard::generateAlternatives(const std::string&,
                                                           const std::string& proposedValue) const {
    std::vector<Alternative> alternatives;
    size_t len = proposedValue.length();
    
    if (len < minLength) {
        // Value too short - pad it
        std::string padded = proposedValue + std::string(minLength - len, '*');
        alternatives.emplace_back(
            padded,
            clampConfidence(0.75),
            RiskLevel::MEDIUM,
            "Padded to minimum length " + std::to_string(minLength)
        );
    } else if (len > maxLength) {
        // Value too long - truncate it
        std::string truncated = proposedValue.substr(0, maxLength);
        alternatives.emplace_back(
            truncated,
            clampConfidence(0.8),
            RiskLevel::MEDIUM,
            "Truncated to maximum length " + std::to_string(maxLength)
        );
        
        // Also suggest truncating to 80% of max
        if (maxLength > 5) {
            size_t shorterLen = maxLength * 4 / 5;
            std::string shorter = proposedValue.substr(0, shorterLen);
            alternatives.emplace_back(
                shorter,
                clampConfidence(0.7),
                RiskLevel::MEDIUM,
                "Truncated to " + std::to_string(shorterLen) + " characters (safer margin)"
            );
        }
    }
    
    return alternatives;
}

std::string LengthGuard::describe() const {
    return "String length: [" + std::to_string(minLength) + ", " + 
           std::to_string(maxLength) + "] characters";
}

// ============= RegexGuard Implementation =============

GuardResult RegexGuard::evaluate(const std::string&,
                                 const std::string& proposedValue,
                                 std::string& reason) const {
    if (std::regex_match(proposedValue, compiled)) {
        reason = "Value matches regex pattern";
        return GuardResult::ACCEPT;
    }

    reason = "Value does not match regex pattern: " + pattern;
    return GuardResult::REJECT;
}

std::vector<Alternative> RegexGuard::generateAlternatives(const std::string&,
                                                          const std::string&) const {
    return {};
}

std::string RegexGuard::describe() const {
    return "Regex pattern: " + pattern;
}

// ============= RateChangeGuard Implementation =============

GuardResult RateChangeGuard::evaluate(const std::string& targetKey,
                                      const std::string& proposedValue,
                                      std::string& reason) const {
    double proposed = 0.0;
    if (!tryParseDouble(proposedValue, proposed)) {
        reason = "Value is not a valid number";
        return GuardResult::REJECT;
    }

    auto previousOpt = getValueForKey(targetKey);
    if (!previousOpt.has_value()) {
        reason = "No previous value to compare";
        return GuardResult::ACCEPT;
    }

    double previous = 0.0;
    if (!tryParseDouble(previousOpt.value(), previous)) {
        reason = "Previous value is not numeric";
        return GuardResult::REJECT;
    }

    const double epsilon = 1e-12;
    if (std::abs(previous) <= epsilon) {
        if (std::abs(proposed) <= epsilon) {
            reason = "No change from zero baseline";
            return GuardResult::ACCEPT;
        }
        reason = "Previous value is zero; percent change exceeds limit";
        return GuardResult::COUNTER_OFFER;
    }

    double percentChange = std::abs((proposed - previous) / previous) * 100.0;
    if (percentChange <= maxPercent) {
        reason = "Change within " + formatDouble(maxPercent) + "% limit";
        return GuardResult::ACCEPT;
    }

    reason = "Change " + formatDouble(percentChange) + "% exceeds " + formatDouble(maxPercent) + "% limit";
    return GuardResult::COUNTER_OFFER;
}

std::vector<Alternative> RateChangeGuard::generateAlternatives(const std::string& targetKey,
                                                               const std::string& proposedValue) const {
    std::vector<Alternative> alternatives;

    double proposed = 0.0;
    if (!tryParseDouble(proposedValue, proposed)) {
        return alternatives;
    }

    auto previousOpt = getValueForKey(targetKey);
    if (!previousOpt.has_value()) {
        return alternatives;
    }

    double previous = 0.0;
    if (!tryParseDouble(previousOpt.value(), previous)) {
        return alternatives;
    }

    const double epsilon = 1e-12;
    if (std::abs(previous) <= epsilon) {
        alternatives.emplace_back(
            formatDouble(0.0),
            clampConfidence(0.9),
            RiskLevel::LOW,
            "No change allowed from zero baseline"
        );
        return alternatives;
    }

    double delta = std::abs(previous) * (maxPercent / 100.0);
    double lower = previous - delta;
    double upper = previous + delta;

    if (proposed < lower) {
        alternatives.emplace_back(
            formatDouble(lower),
            clampConfidence(0.9),
            RiskLevel::LOW,
            "Clamped to maximum allowed decrease"
        );
    } else if (proposed > upper) {
        alternatives.emplace_back(
            formatDouble(upper),
            clampConfidence(0.9),
            RiskLevel::LOW,
            "Clamped to maximum allowed increase"
        );
    }

    return alternatives;
}

std::string RateChangeGuard::describe() const {
    return "Max change: " + formatDouble(maxPercent) + "% from previous value";
}

// ============= CrossKeyGuard Implementation =============

GuardResult CrossKeyGuard::evaluate(const std::string&,
                                    const std::string& proposedValue,
                                    std::string& reason) const {
    double proposed = 0.0;
    if (!tryParseDouble(proposedValue, proposed)) {
        reason = "Value is not a valid number";
        return GuardResult::REJECT;
    }

    auto otherOpt = getValueForKey(otherKey);
    if (!otherOpt.has_value()) {
        reason = "Other key not found: " + otherKey;
        return GuardResult::REJECT;
    }

    double otherValue = 0.0;
    if (!tryParseDouble(otherOpt.value(), otherValue)) {
        reason = "Other key value is not numeric: " + otherKey;
        return GuardResult::REJECT;
    }

    double maxAllowed = otherValue * factor;
    if (proposed <= maxAllowed) {
        reason = "Value within cross-key limit";
        return GuardResult::ACCEPT;
    }

    reason = "Value exceeds cross-key limit (" + otherKey + " * " + formatDouble(factor) + ")";
    return GuardResult::COUNTER_OFFER;
}

std::vector<Alternative> CrossKeyGuard::generateAlternatives(const std::string&,
                                                             const std::string& proposedValue) const {
    std::vector<Alternative> alternatives;

    double proposed = 0.0;
    if (!tryParseDouble(proposedValue, proposed)) {
        return alternatives;
    }

    auto otherOpt = getValueForKey(otherKey);
    if (!otherOpt.has_value()) {
        return alternatives;
    }

    double otherValue = 0.0;
    if (!tryParseDouble(otherOpt.value(), otherValue)) {
        return alternatives;
    }

    double maxAllowed = otherValue * factor;
    if (proposed > maxAllowed) {
        alternatives.emplace_back(
            formatDouble(maxAllowed),
            clampConfidence(0.9),
            RiskLevel::LOW,
            "Clamped to cross-key limit"
        );
    }

    return alternatives;
}

std::string CrossKeyGuard::describe() const {
    return "Cross-key limit: value <= " + otherKey + " * " + formatDouble(factor);
}
