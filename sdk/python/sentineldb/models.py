from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class Version:
    value: str
    timestamp: str

@dataclass
class Alternative:
    value: str
    explanation: str

@dataclass
class ProposalResult:
    key: str
    proposed_value: str
    status: str
    reason: str
    stored_value: Optional[str] = None
    alternatives: List[Alternative] = field(default_factory=list)
    triggered_guards: List[str] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return self.status == "ACCEPT"

    @property
    def rejected(self) -> bool:
        return self.status == "REJECT"

    @property
    def has_alternatives(self) -> bool:
        return len(self.alternatives) > 0

@dataclass
class Guard:
    name: str
    key_pattern: str
    description: str
    enabled: bool = True

@dataclass
class HealthStatus:
    status: str
    keys: int
    wal_enabled: bool
    version: str
