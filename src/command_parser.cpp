#include "command_parser.h"
#include <sstream>
#include <algorithm>
#include <cctype>

Command CommandParser::parse(const std::string& input) {
    Command cmd;
    
    // Tokenize the input
    std::vector<std::string> tokens = tokenize(input);
    
    // Empty input
    if (tokens.empty()) {
        cmd.type = CommandType::INVALID;
        return cmd;
    }
    
    // Special case: GET key AT timestamp
    if (tokens.size() >= 4 && getCommandType(tokens[0]) == CommandType::GET) {
        std::string atToken = tokens[2];
        std::transform(atToken.begin(), atToken.end(), atToken.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        if (atToken == "AT") {
            cmd.type = CommandType::GETAT;
            // args: [key, timestamp]
            cmd.args.push_back(tokens[1]);
            // Join remaining tokens as timestamp (might have spaces in ISO format)
            for (size_t i = 3; i < tokens.size(); ++i) {
                if (i > 3) cmd.args.back() += " ";
                else cmd.args.push_back("");
                cmd.args.back() += tokens[i];
            }
            return cmd;
        }
    }
    
    // Special case: EXPLAIN GET key AT timestamp
    if (tokens.size() >= 5 && getCommandType(tokens[0]) == CommandType::EXPLAIN) {
        std::string getToken = tokens[1];
        std::string atToken = tokens[3];
        std::transform(getToken.begin(), getToken.end(), getToken.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        std::transform(atToken.begin(), atToken.end(), atToken.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        if (getToken == "GET" && atToken == "AT") {
            cmd.type = CommandType::EXPLAIN;
            // args: [key, timestamp]
            cmd.args.push_back(tokens[2]);
            // Join remaining tokens as timestamp
            for (size_t i = 4; i < tokens.size(); ++i) {
                if (i > 4) cmd.args.back() += " ";
                else cmd.args.push_back("");
                cmd.args.back() += tokens[i];
            }
            return cmd;
        }
    }
    
    // Get command type from first token
    cmd.type = getCommandType(tokens[0]);

    if (cmd.type == CommandType::SET) {
        if (tokens.size() > 1) {
            cmd.args.push_back(tokens[1]);
            if (tokens.size() > 2) {
                std::string value;
                for (size_t i = 2; i < tokens.size(); ++i) {
                    if (i > 2) value += " ";
                    value += tokens[i];
                }
                cmd.args.push_back(value);
            }
        }
        return cmd;
    }
    
    // Store remaining tokens as arguments
    if (tokens.size() > 1) {
        cmd.args = std::vector<std::string>(tokens.begin() + 1, tokens.end());
    }
    
    return cmd;
}

std::vector<std::string> CommandParser::tokenize(const std::string& input) {
    std::vector<std::string> tokens;
    std::string current;
    bool inQuotes = false;
    bool escaping = false;

    for (char c : input) {
        if (escaping) {
            current.push_back(c);
            escaping = false;
            continue;
        }

        if (c == '\\') {
            escaping = true;
            continue;
        }

        if (c == '"') {
            inQuotes = !inQuotes;
            continue;
        }

        if (std::isspace(static_cast<unsigned char>(c)) && !inQuotes) {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
            continue;
        }

        current.push_back(c);
    }

    if (!current.empty()) {
        tokens.push_back(current);
    }
    
    return tokens;
}

CommandType CommandParser::getCommandType(const std::string& cmd) {
    // Convert to uppercase for case-insensitive comparison
    std::string upper = cmd;
    std::transform(upper.begin(), upper.end(), upper.begin(), 
                   [](unsigned char c) { return std::toupper(c); });
    
    if (upper == "SET") return CommandType::SET;
    if (upper == "GET") return CommandType::GET;
    if (upper == "DEL") return CommandType::DEL;
    if (upper == "HISTORY") return CommandType::HISTORY;
    if (upper == "SNAPSHOT") return CommandType::SNAPSHOT;
    if (upper == "CONFIG") return CommandType::CONFIG;
    if (upper == "EXPLAIN") return CommandType::EXPLAIN;
    if (upper == "PROPOSE") return CommandType::PROPOSE;
    if (upper == "GUARD") return CommandType::GUARD;
    if (upper == "POLICY") return CommandType::POLICY;
    if (upper == "EXIT" || upper == "QUIT") return CommandType::EXIT;
    
    return CommandType::INVALID;
}
