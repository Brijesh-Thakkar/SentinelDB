#ifndef METRICS_H
#define METRICS_H

#include <atomic>
#include <string>
#include <sstream>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <iomanip>

class Metrics {
public:
    static Metrics& instance() {
        static Metrics inst;
        return inst;
    }

    void recordRequest(const std::string& endpoint, const std::string& status) {
        std::lock_guard<std::mutex> lock(mutex_);
        requestCounts_[endpoint + ":" + status]++;
        totalRequests_++;
    }

    void recordLatency(const std::string& endpoint, double latencyMs) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& stats = latencyStats_[endpoint];
        stats.count++;
        stats.totalMs += latencyMs;
    }

    void setWalSize(size_t bytes) {
        walSizeBytes_.store(bytes, std::memory_order_relaxed);
    }

    void setActiveKeys(size_t count) {
        activeKeys_.store(count, std::memory_order_relaxed);
    }

    std::string toPrometheusFormat() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostringstream ss;

        ss << "# HELP sentineldb_requests_total Total HTTP requests by endpoint and status\n";
        ss << "# TYPE sentineldb_requests_total counter\n";
        for (const auto& [key, count] : requestCounts_) {
            size_t colonPos = key.find(':');
            std::string endpoint = key.substr(0, colonPos);
            std::string status = key.substr(colonPos + 1);
            ss << "sentineldb_requests_total{endpoint=\"" << endpoint
               << "\",status=\"" << status << "\"} " << count << "\n";
        }

        ss << "\n# HELP sentineldb_request_latency_ms_avg Average request latency in ms\n";
        ss << "# TYPE sentineldb_request_latency_ms_avg gauge\n";
        for (const auto& [endpoint, stats] : latencyStats_) {
            if (stats.count > 0) {
                double avg = stats.totalMs / static_cast<double>(stats.count);
                ss << "sentineldb_request_latency_ms_avg{endpoint=\""
                   << endpoint << "\"} " << std::fixed << std::setprecision(3) << avg << "\n";
            }
        }

        ss << "\n# HELP sentineldb_wal_size_bytes WAL file size in bytes\n";
        ss << "# TYPE sentineldb_wal_size_bytes gauge\n";
        ss << "sentineldb_wal_size_bytes "
           << walSizeBytes_.load(std::memory_order_relaxed) << "\n";

        ss << "\n# HELP sentineldb_active_keys_total Number of keys currently stored\n";
        ss << "# TYPE sentineldb_active_keys_total gauge\n";
        ss << "sentineldb_active_keys_total "
           << activeKeys_.load(std::memory_order_relaxed) << "\n";

        ss << "\n# HELP sentineldb_total_requests Total requests processed since startup\n";
        ss << "# TYPE sentineldb_total_requests counter\n";
        ss << "sentineldb_total_requests " << totalRequests_.load() << "\n";

        return ss.str();
    }

private:
    Metrics() : walSizeBytes_(0), activeKeys_(0), totalRequests_(0) {}
    Metrics(const Metrics&) = delete;
    Metrics& operator=(const Metrics&) = delete;

    struct LatencyStats {
        uint64_t count = 0;
        double totalMs = 0.0;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, uint64_t> requestCounts_;
    std::unordered_map<std::string, LatencyStats> latencyStats_;
    std::atomic<size_t> walSizeBytes_;
    std::atomic<size_t> activeKeys_;
    std::atomic<uint64_t> totalRequests_;
};

// RAII timer — records latency automatically on destruction
class RequestTimer {
public:
    explicit RequestTimer(const std::string& endpoint)
        : endpoint_(endpoint),
          start_(std::chrono::steady_clock::now()) {}

    ~RequestTimer() {
        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start_).count();
        Metrics::instance().recordLatency(endpoint_, ms);
    }

    // Non-copyable
    RequestTimer(const RequestTimer&) = delete;
    RequestTimer& operator=(const RequestTimer&) = delete;

private:
    std::string endpoint_;
    std::chrono::steady_clock::time_point start_;
};

#endif // METRICS_H
