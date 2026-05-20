#ifndef REPLAY_SERVICE_H
#define REPLAY_SERVICE_H

#include <memory>
#include <vector>
#include <string>
#include "kvstore.h"
#include "wal.h"

struct ReplayStats {
    bool snapshotApplied{false};
    size_t snapshotCommands{0};
    size_t walCommands{0};
    size_t keysAfterSnapshot{0};
    size_t keysAfterWal{0};
};

class ReplayService {
public:
    static ReplayStats replay(const std::shared_ptr<KVStore>& kvstore,
                              const std::shared_ptr<WAL>& wal);

private:
    static void replayConfigCommands(const std::vector<std::string>& commands,
                                     const std::shared_ptr<KVStore>& kvstore);
    static void replayDataCommands(const std::vector<std::string>& commands,
                                   const std::shared_ptr<KVStore>& kvstore,
                                   std::optional<std::chrono::system_clock::time_point> defaultTimestamp);
};

#endif // REPLAY_SERVICE_H
