import requests
from typing import Optional, List
from .models import Version, ProposalResult, Alternative, Guard, HealthStatus
from .exceptions import (
    SentinelDBError, ConnectionError, KeyNotFoundError, GuardViolationError
)

class SentinelDB:
    """
    Python client for SentinelDB — the negotiating key-value store.

    Usage:
        db = SentinelDB("http://localhost:8080")
        db.set("score", "95")
        value = db.get("score")
        result = db.propose("score", "150")
        if result.has_alternatives:
            db.set("score", result.alternatives[0].value)
    """

    def __init__(self, url: str, timeout: int = 10, api_key: str = None):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        if api_key:
            self.session.headers["X-API-Key"] = api_key
        self.session.headers["Content-Type"] = "application/json"

    def _request(self, method: str, path: str, **kwargs) -> dict:
        try:
            resp = self.session.request(
                method,
                f"{self.url}{path}",
                timeout=self.timeout,
                **kwargs
            )
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(self.url, e)
        except requests.exceptions.Timeout:
            raise SentinelDBError(f"Request timed out after {self.timeout}s")

        if resp.status_code == 404:
            data = resp.json()
            raise KeyNotFoundError(data.get("key", "unknown"))

        if not resp.ok:
            raise SentinelDBError(f"HTTP {resp.status_code}: {resp.text}")

        return resp.json()

    # ── Core Operations ──────────────────────────────────────────

    def set(self, key: str, value: str) -> None:
        """Set a key-value pair. Bypasses guards."""
        self._request("POST", "/set", json={"key": key, "value": value})

    def get(self, key: str) -> str:
        """Get the current value of a key."""
        data = self._request("GET", "/get", params={"key": key})
        return data["value"]

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        try:
            self.get(key)
            return True
        except KeyNotFoundError:
            return False

    # ── Temporal Operations ──────────────────────────────────────

    def history(self, key: str) -> List[Version]:
        """Get full version history of a key."""
        data = self._request("GET", "/history", params={"key": key})
        return [
            Version(value=v["value"], timestamp=v["timestamp"])
            for v in data.get("versions", [])
        ]

    def get_at(self, key: str, timestamp: str) -> Optional[str]:
        """Get value of key at a specific timestamp."""
        try:
            data = self._request("GET", "/getAt",
                                 params={"key": key, "timestamp": timestamp})
            return data.get("value")
        except KeyNotFoundError:
            return None

    # ── Guard & Proposal Operations ──────────────────────────────

    def propose(self, key: str, value: str) -> ProposalResult:
        """
        Propose a write without committing.
        Returns ProposalResult with status and alternatives.
        """
        data = self._request("POST", "/propose", json={"key": key, "value": value})
        alternatives = [
            Alternative(value=alt["value"], explanation=alt.get("explanation", ""))
            for alt in data.get("alternatives", [])
        ]
        return ProposalResult(
            key=key,
            proposed_value=value,
            status=data.get("result", "REJECT"),
            reason=data.get("reason", ""),
            alternatives=alternatives,
            triggered_guards=data.get("triggeredGuards", [])
        )

    def safe_set(self, key: str, value: str,
                 use_first_alternative: bool = True) -> ProposalResult:
        """
        Propose then commit. If counter-offered, uses first alternative.
        Raises GuardViolationError if rejected with no alternatives.
        """
        result = self.propose(key, value)
        if result.accepted:
            self.set(key, value)
            return result
        if result.has_alternatives and use_first_alternative:
            self.set(key, result.alternatives[0].value)
            return result
        raise GuardViolationError(key, result.reason, result.alternatives)

    # ── Guard Management ─────────────────────────────────────────

    def add_guard(self, name: str, key_pattern: str,
                  guard_type: str, **params) -> None:
        """Add a guard constraint."""
        payload = {
            "name": name,
            "keyPattern": key_pattern,
            "type": guard_type,
            **{k: str(v) for k, v in params.items()}
        }
        self._request("POST", "/guards", json=payload)

    def list_guards(self) -> List[Guard]:
        """List all active guards."""
        data = self._request("GET", "/guards")
        return [
            Guard(
                name=g["name"],
                key_pattern=g["keyPattern"],
                description=g.get("description", ""),
                enabled=g.get("enabled", True)
            )
            for g in data.get("guards", [])
        ]

    # ── Policy Management ────────────────────────────────────────

    def set_policy(self, policy: str) -> None:
        """Set decision policy: STRICT, SAFE_DEFAULT, or DEV_FRIENDLY."""
        valid = {"STRICT", "SAFE_DEFAULT", "DEV_FRIENDLY"}
        if policy not in valid:
            raise ValueError(f"Policy must be one of {valid}")
        self._request("POST", "/policy", json={"policy": policy})

    def get_policy(self) -> str:
        """Get current decision policy."""
        data = self._request("GET", "/policy")
        return data.get("activePolicy", "SAFE_DEFAULT")

    # ── Observability ────────────────────────────────────────────

    def health(self) -> HealthStatus:
        """Check server health."""
        data = self._request("GET", "/health")
        return HealthStatus(
            status=data["status"],
            keys=data.get("keys", 0),
            wal_enabled=data.get("wal_enabled", False),
            version=data.get("version", "unknown")
        )

    def metrics(self) -> str:
        """Get Prometheus metrics as raw text."""
        resp = self.session.get(f"{self.url}/metrics", timeout=self.timeout)
        return resp.text

    # ── Context Manager ──────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

    def __repr__(self):
        return f"SentinelDB(url={self.url!r})"
