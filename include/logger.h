#ifndef LOGGER_H
#define LOGGER_H

#include <memory>
#include <string>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace SentinelDB {

inline void initLogger() {
    try {
        // Only create if not already exists
        if (!spdlog::get("sentineldb")) {
            auto console = spdlog::stdout_color_mt("sentineldb");
            // Format: [2026-03-18 13:42:26.741] [info] message
            console->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
            console->set_level(spdlog::level::info);
            spdlog::set_default_logger(console);
        }
    } catch (const spdlog::spdlog_ex& ex) {
        // If logger already exists, just set it as default
        auto existing = spdlog::get("sentineldb");
        if (existing) spdlog::set_default_logger(existing);
    }
}

} // namespace SentinelDB

#endif // LOGGER_H
