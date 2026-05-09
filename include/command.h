#ifndef COMMAND_H
#define COMMAND_H

#include <string>
#include <vector>

enum class CommandType { SET, GET, GETAT, DIFF, ROLLBACK, AUDIT, DEL, HISTORY, SNAPSHOT, CONFIG, EXPLAIN, 
                         PROPOSE, GUARD, POLICY, EXIT, INVALID };

struct Command {
  CommandType type;
  std::vector<std::string> args;
};

#endif // COMMAND_H
