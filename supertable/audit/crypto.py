# route: supertable.audit.crypto
"""
Fernet encryption/decryption for sensitive audit detail fields.

Used by the public audit event path to protect SQL text in ``detail``.
Configured deployments store only a SHA-256 digest and Fernet ciphertext;
unconfigured deployments retain the digest but redact the reversible text.

Key management:
  - SUPERTABLE_AUDIT_FERNET_KEY env var holds the base64-encoded key
  - If empty, sensitive SQL text is redacted rather than stored in plaintext
  - Key rotation: generate a new key, update env, restart servers.
    Old events remain readable with the old key until re-encrypted.

Compliance: DORA Art. 6 (confidentiality), SOC 2 CC6.1 (data protection).
"""
from __future__ import annotations

import logging
import hashlib
import json
import math
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

class AuditEncryptionError(RuntimeError):
    """Raised when configured audit encryption cannot protect plaintext."""


_fernet_instance: Any = None
_fernet_loaded = False
_fernet_key: str | bytes | None = None
_fernet_lock = threading.Lock()
_SENSITIVE_DETAIL_FIELDS = frozenset({
    "query_text",
    "sql",
    "sql_text",
    "statement",
})
_RESERVED_SENSITIVE_OUTPUT_FIELDS = frozenset(
    f"{field}_{suffix}"
    for field in _SENSITIVE_DETAIL_FIELDS
    for suffix in ("encrypted", "redacted", "sha256")
)
_MAX_DETAIL_PROTECTION_DEPTH = 16
_MAX_DETAIL_NODES = 1_024
_MAX_DETAIL_CONTAINER_ITEMS = 256
_MAX_DETAIL_STRING_BYTES = 32 * 1_024
_MAX_DETAIL_SERIALIZED_BYTES = 64 * 1_024
_MAX_DETAIL_INTEGER_BITS = 4_096


class _DetailBudget:
    def __init__(self) -> None:
        self.nodes = 0
        self.string_bytes = 0

    def consume_node(self) -> None:
        self.nodes += 1
        if self.nodes > _MAX_DETAIL_NODES:
            raise AuditEncryptionError(
                "audit detail exceeds the structural node limit"
            )

    def validate_string(self, value: str, *, label: str) -> None:
        try:
            size = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            raise AuditEncryptionError(
                f"audit detail {label} is not valid UTF-8"
            ) from None
        if size > _MAX_DETAIL_STRING_BYTES:
            raise AuditEncryptionError(
                f"audit detail {label} exceeds the string byte limit"
            )
        self.string_bytes += size
        if self.string_bytes > _MAX_DETAIL_SERIALIZED_BYTES:
            raise AuditEncryptionError(
                "audit detail exceeds the cumulative input byte limit"
            )


def _get_fernet():
    """Lazy-load Fernet, returning ``None`` only when no key is configured.

    A configured key is an explicit confidentiality policy.  Missing runtime
    dependencies, malformed keys, and configuration-load failures therefore
    raise :class:`AuditEncryptionError` instead of silently storing plaintext.
    """
    global _fernet_instance, _fernet_key, _fernet_loaded
    try:
        from supertable.config.settings import settings
    except Exception:
        raise AuditEncryptionError(
            "audit encryption configuration is unavailable; refusing "
            "plaintext fallback"
        ) from None

    try:
        raw_key = settings.SUPERTABLE_AUDIT_FERNET_KEY
    except Exception:
        raise AuditEncryptionError(
            "audit encryption configuration is unavailable; refusing "
            "plaintext fallback"
        ) from None
    if raw_key is not None and not isinstance(raw_key, (str, bytes)):
        raise AuditEncryptionError(
            "SUPERTABLE_AUDIT_FERNET_KEY must be text or bytes; refusing "
            "plaintext fallback"
        )
    normalized_key: str | bytes | None
    try:
        if isinstance(raw_key, str):
            normalized_key = str.strip(raw_key)
        elif isinstance(raw_key, bytes):
            normalized_key = bytes.strip(raw_key)
        else:
            normalized_key = None
    except Exception:
        raise AuditEncryptionError(
            "configured audit encryption key is invalid; refusing "
            "plaintext fallback"
        ) from None
    if _fernet_loaded and normalized_key == _fernet_key:
        return _fernet_instance

    with _fernet_lock:
        if _fernet_loaded and normalized_key == _fernet_key:
            return _fernet_instance

        if raw_key is None or raw_key == "" or raw_key == b"":
            _fernet_instance = None
            _fernet_key = normalized_key
            _fernet_loaded = True
            logger.debug(
                "[audit-crypto] No SUPERTABLE_AUDIT_FERNET_KEY set — "
                "sensitive audit fields are redacted"
            )
            return None
        key = normalized_key
        if not key:
            _fernet_instance = None
            _fernet_key = key
            _fernet_loaded = True
            logger.debug(
                "[audit-crypto] Empty SUPERTABLE_AUDIT_FERNET_KEY — "
                "sensitive audit fields are redacted"
            )
            return None

        try:
            from cryptography.fernet import Fernet
        except ImportError:
            logger.critical(
                "[audit-crypto] cryptography dependency unavailable while "
                "SUPERTABLE_AUDIT_FERNET_KEY is configured"
            )
            raise AuditEncryptionError(
                "cryptography is required when SUPERTABLE_AUDIT_FERNET_KEY "
                "is configured; refusing plaintext fallback"
            ) from None

        try:
            instance = Fernet(
                key.encode("utf-8") if isinstance(key, str) else key
            )
        except Exception:
            logger.critical(
                "[audit-crypto] configured Fernet key is invalid; refusing "
                "plaintext fallback"
            )
            raise AuditEncryptionError(
                "failed to initialize configured audit encryption; refusing "
                "plaintext fallback"
            ) from None

        _fernet_instance = instance
        _fernet_key = key
        _fernet_loaded = True
        logger.info("[audit-crypto] Fernet encryption initialized for audit fields")
        return _fernet_instance


def encrypt_field(plaintext: str) -> str:
    """Encrypt a string field. Returns the Fernet token as a UTF-8 string.

    Plaintext is returned only when encryption is deliberately unconfigured.
    If a configured key cannot be initialized or used, the operation fails
    closed with :class:`AuditEncryptionError`.
    """
    if not plaintext:
        return ""
    f = _get_fernet()
    if f is None:
        return plaintext
    try:
        token = f.encrypt(plaintext.encode("utf-8"))
        return token.decode("utf-8")
    except Exception:
        logger.critical(
            "[audit-crypto] configured encryption failed; refusing plaintext "
            "fallback"
        )
        raise AuditEncryptionError(
            "configured audit encryption failed; refusing plaintext fallback"
        ) from None


def ensure_configured_encryption_ready() -> None:
    """Validate the configured key and dependency before accepting events."""
    fernet = _get_fernet()
    if fernet is None:
        raise AuditEncryptionError(
            "audit encryption was declared configured but no key is available"
        )
    probe = b"supertable-audit-encryption-self-test"
    try:
        token = fernet.encrypt(probe)
        recovered = fernet.decrypt(token)
    except Exception:
        raise AuditEncryptionError(
            "configured audit encryption failed its startup self-test"
        ) from None
    if recovered != probe:
        raise AuditEncryptionError(
            "configured audit encryption failed its startup self-test"
        )


def _sensitive_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError):
        raise AuditEncryptionError(
            "audit sensitive detail is not canonical JSON"
        ) from None


def _protected_sensitive_fields(name: str, value: Any) -> dict[str, Any]:
    plaintext = _sensitive_text(value)
    protected: dict[str, Any] = {
        f"{name}_sha256": hashlib.sha256(
            plaintext.encode("utf-8")
        ).hexdigest(),
    }
    if is_encryption_available():
        protected[f"{name}_encrypted"] = encrypt_field(plaintext)
    else:
        protected[f"{name}_redacted"] = True
    return protected


def _validate_detail_value(
    value: Any,
    *,
    depth: int,
    budget: _DetailBudget,
) -> None:
    if depth > _MAX_DETAIL_PROTECTION_DEPTH:
        raise AuditEncryptionError(
            "audit detail exceeds the protection nesting limit"
        )
    budget.consume_node()
    if isinstance(value, dict):
        if len(value) > _MAX_DETAIL_CONTAINER_ITEMS:
            raise AuditEncryptionError(
                "audit detail exceeds the container item limit"
            )
        for raw_name, item in value.items():
            if not isinstance(raw_name, str):
                raise AuditEncryptionError(
                    "audit detail object keys must be strings"
                )
            budget.validate_string(raw_name, label="key")
            _validate_detail_value(
                item, depth=depth + 1, budget=budget,
            )
        return
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_DETAIL_CONTAINER_ITEMS:
            raise AuditEncryptionError(
                "audit detail exceeds the container item limit"
            )
        for item in value:
            _validate_detail_value(
                item, depth=depth + 1, budget=budget,
            )
        return
    if isinstance(value, str):
        budget.validate_string(value, label="string")
        return
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if value.bit_length() > _MAX_DETAIL_INTEGER_BITS:
            raise AuditEncryptionError(
                "audit detail integer exceeds the numeric limit"
            )
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AuditEncryptionError(
                "audit detail contains a non-finite number"
            )
        return
    raise AuditEncryptionError(
        "audit detail contains a non-JSON value"
    )


def _protect_detail_value(
    value: Any,
    *,
    depth: int,
    budget: _DetailBudget,
) -> Any:
    if depth > _MAX_DETAIL_PROTECTION_DEPTH:
        raise AuditEncryptionError(
            "audit detail exceeds the protection nesting limit"
        )
    budget.consume_node()
    if isinstance(value, dict):
        if len(value) > _MAX_DETAIL_CONTAINER_ITEMS:
            raise AuditEncryptionError(
                "audit detail exceeds the container item limit"
            )
        protected: dict[str, Any] = {}
        seen_sensitive: set[str] = set()
        for raw_name, item in value.items():
            if not isinstance(raw_name, str):
                raise AuditEncryptionError(
                    "audit detail object keys must be strings"
                )
            name = raw_name
            budget.validate_string(name, label="key")
            normalized = name.casefold()
            if normalized in _RESERVED_SENSITIVE_OUTPUT_FIELDS:
                raise AuditEncryptionError(
                    "audit detail contains reserved sensitive-field output names"
                )
            if normalized in _SENSITIVE_DETAIL_FIELDS:
                if normalized in seen_sensitive:
                    raise AuditEncryptionError(
                        "audit detail contains duplicate sensitive fields"
                    )
                seen_sensitive.add(normalized)
                _validate_detail_value(
                    item, depth=depth + 1, budget=budget,
                )
                protected.update(
                    _protected_sensitive_fields(normalized, item)
                )
                continue
            protected[name] = _protect_detail_value(
                item, depth=depth + 1, budget=budget,
            )
        return protected
    if isinstance(value, list):
        if len(value) > _MAX_DETAIL_CONTAINER_ITEMS:
            raise AuditEncryptionError(
                "audit detail exceeds the container item limit"
            )
        return [
            _protect_detail_value(
                item, depth=depth + 1, budget=budget,
            )
            for item in value
        ]
    if isinstance(value, tuple):
        if len(value) > _MAX_DETAIL_CONTAINER_ITEMS:
            raise AuditEncryptionError(
                "audit detail exceeds the container item limit"
            )
        return [
            _protect_detail_value(
                item, depth=depth + 1, budget=budget,
            )
            for item in value
        ]
    if isinstance(value, str):
        budget.validate_string(value, label="string")
        return value
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value.bit_length() > _MAX_DETAIL_INTEGER_BITS:
            raise AuditEncryptionError(
                "audit detail integer exceeds the numeric limit"
            )
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AuditEncryptionError(
                "audit detail contains a non-finite number"
            )
        return value
    raise AuditEncryptionError(
        "audit detail contains a non-JSON value"
    )


def _validate_serialized_detail_size(value: Any) -> None:
    try:
        canonical = json.dumps(
            value,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise AuditEncryptionError(
            "audit detail is not canonical JSON"
        ) from None
    if len(canonical) > _MAX_DETAIL_SERIALIZED_BYTES:
        raise AuditEncryptionError(
            "audit detail exceeds the serialized byte limit"
        )


def protect_sensitive_detail(detail: Any, *, action: str) -> Any:
    """Return an audit detail payload with reversible SQL text protected.

    Dict payloads and pre-serialized JSON objects are inspected recursively for
    every action.  ``query_execute`` additionally accepts the legacy convention
    where the complete detail string is the SQL text.
    Caller-supplied ``*_encrypted``, ``*_redacted``, and ``*_sha256`` output
    names are reserved and rejected recursively, even without a matching raw
    field, so plaintext cannot masquerade as trusted protected output.
    """
    if detail is None:
        return detail
    budget = _DetailBudget()
    if isinstance(detail, (dict, list, tuple)):
        protected = _protect_detail_value(
            detail, depth=0, budget=budget,
        )
        _validate_serialized_detail_size(protected)
        return protected
    if not isinstance(detail, str):
        _validate_detail_value(detail, depth=0, budget=budget)
        _validate_serialized_detail_size(detail)
        return detail
    budget.consume_node()
    budget.validate_string(detail, label="string")
    if not detail:
        return detail
    try:
        parsed = json.loads(detail)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, (dict, list)):
        budget = _DetailBudget()
        protected = _protect_detail_value(
            parsed, depth=0, budget=budget,
        )
        _validate_serialized_detail_size(protected)
        return protected
    if action != "query_execute":
        _validate_serialized_detail_size(detail)
        return detail
    budget = _DetailBudget()
    protected = _protect_detail_value(
        {"sql": detail}, depth=0, budget=budget,
    )
    _validate_serialized_detail_size(protected)
    return protected


def decrypt_field(ciphertext: str) -> Optional[str]:
    """Decrypt a Fernet-encrypted field. Return plaintext or ``None`` on failure.

    Deliberately unconfigured deployments retain legacy pass-through behavior.
    Once Fernet is configured, invalid or attacker-controlled tokens never
    pass through as apparent plaintext.
    """
    if not ciphertext:
        return ""
    f = _get_fernet()
    if f is None:
        return ciphertext
    try:
        plaintext = f.decrypt(ciphertext.encode("utf-8"))
        return plaintext.decode("utf-8")
    except Exception:
        return None


def is_encryption_available() -> bool:
    """Check if Fernet encryption is configured and functional."""
    return _get_fernet() is not None
