# route: supertable.audit.crypto
"""
Fernet encryption/decryption for sensitive audit detail fields.

Used to encrypt full SQL text in query_execute events. The encrypted
value is stored in detail.sql_encrypted; the plaintext preview
(first 200 chars) remains unencrypted for dashboard display.

Key management:
  - SUPERTABLE_AUDIT_FERNET_KEY env var holds the base64-encoded key
  - If empty, full SQL is stored in plaintext (on-premise fallback)
  - Key rotation: generate a new key, update env, restart servers.
    Old events remain readable with the old key until re-encrypted.

Compliance: DORA Art. 6 (confidentiality), SOC 2 CC6.1 (data protection).
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_fernet_instance = None
_fernet_loaded = False


def _get_fernet():
    """Lazy-load Fernet instance from settings. Returns None if unconfigured."""
    global _fernet_instance, _fernet_loaded
    if _fernet_loaded:
        return _fernet_instance
    _fernet_loaded = True

    try:
        from supertable.config.settings import settings
        key = (settings.SUPERTABLE_AUDIT_FERNET_KEY or "").strip()
        if not key:
            logger.debug("[audit-crypto] No SUPERTABLE_AUDIT_FERNET_KEY set — SQL stored in plaintext")
            return None
        from cryptography.fernet import Fernet
        _fernet_instance = Fernet(key.encode("utf-8") if isinstance(key, str) else key)
        logger.info("[audit-crypto] Fernet encryption initialized for audit fields")
    except ImportError:
        logger.warning("[audit-crypto] cryptography package not installed — SQL stored in plaintext")
    except Exception as e:
        logger.error("[audit-crypto] Failed to initialize Fernet: %s — SQL stored in plaintext", e)

    return _fernet_instance


def encrypt_field(plaintext: str) -> str:
    """Encrypt a string field. Returns the Fernet token as a UTF-8 string.

    If encryption is unavailable (no key, no cryptography package),
    returns the plaintext unchanged.
    """
    if not plaintext:
        return ""
    f = _get_fernet()
    if f is None:
        return plaintext
    try:
        token = f.encrypt(plaintext.encode("utf-8"))
        return token.decode("utf-8")
    except Exception as e:
        logger.warning("[audit-crypto] Encryption failed: %s — storing plaintext", e)
        return plaintext


def decrypt_field(ciphertext: str) -> Optional[str]:
    """Decrypt a Fernet-encrypted field. Returns plaintext or None on failure.

    If the value is not a valid Fernet token (e.g. stored as plaintext
    because encryption was disabled), returns the value as-is.
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
        # Not a valid Fernet token — likely stored as plaintext
        return ciphertext


def is_encryption_available() -> bool:
    """Check if Fernet encryption is configured and functional."""
    return _get_fernet() is not None
