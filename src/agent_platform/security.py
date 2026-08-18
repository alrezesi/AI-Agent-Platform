"""Security helpers shared across the platform."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any


def hash_api_key(api_key: str) -> str:
    """Return a stable SHA-256 hash for an API key."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def api_key_matches(stored_hash: str, api_key: str) -> bool:
    """Compare an API key against a stored hash without leaking timing."""
    candidate = hash_api_key(api_key)
    if not isinstance(stored_hash, str) or len(stored_hash) != len(candidate):
        return False
    return hmac.compare_digest(stored_hash, candidate)


def stored_api_key_hash(key_record: Mapping[str, Any]) -> str | None:
    """Return the hash that should be used to look up a stored key record."""
    stored_hash = key_record.get("key_hash")
    if isinstance(stored_hash, str) and stored_hash:
        return stored_hash
    legacy_key = key_record.get("key")
    if isinstance(legacy_key, str) and legacy_key:
        return hash_api_key(legacy_key)
    return None


def api_key_record_matches(key_record: Mapping[str, Any], api_key: str) -> bool:
    """Return True if an active key record matches the provided API key."""
    if not key_record.get("is_active", True):
        return False
    stored_hash = stored_api_key_hash(key_record)
    return isinstance(stored_hash, str) and api_key_matches(stored_hash, api_key)
