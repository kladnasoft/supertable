# route: supertable.audit.tests.test_crypto
"""Tests for ``supertable.audit.crypto``.

Covers:
  - Round-trip encrypt → decrypt with a configured Fernet key
  - Empty-string handling on both encrypt and decrypt
  - Plaintext fallback when the key is missing or empty
  - Plaintext pass-through on decrypt for non-Fernet payloads (e.g. legacy data)
  - ``is_encryption_available`` reports the correct state
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

# cryptography is a transitive dependency we already exercise in the audit
# pipeline; require it for these tests.
cryptography = pytest.importorskip("cryptography")
from cryptography.fernet import Fernet  # noqa: E402  (after importorskip)

from supertable.audit import crypto


@pytest.fixture(autouse=True)
def reset_crypto_module_state() -> None:
    """Force every test to re-initialize the lazy module-level cache."""
    crypto._fernet_instance = None
    crypto._fernet_loaded = False
    yield
    crypto._fernet_instance = None
    crypto._fernet_loaded = False


@pytest.fixture
def with_fernet_key(monkeypatch: pytest.MonkeyPatch) -> str:
    """Provide a real Fernet key via the (mocked) settings object."""
    key = Fernet.generate_key().decode("utf-8")
    fake_settings = SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=key)
    # crypto._get_fernet does ``from supertable.config.settings import settings``,
    # so we shim that attribute on the real module.
    import supertable.config.settings as settings_module

    monkeypatch.setattr(settings_module, "settings", fake_settings, raising=True)
    return key


@pytest.fixture
def without_fernet_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``SUPERTABLE_AUDIT_FERNET_KEY`` to be empty."""
    fake_settings = SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY="")
    import supertable.config.settings as settings_module

    monkeypatch.setattr(settings_module, "settings", fake_settings, raising=True)


# ---------------------------------------------------------------------------
# With encryption configured
# ---------------------------------------------------------------------------


class TestWithFernetKey:
    def test_round_trip(self, with_fernet_key: str) -> None:
        plaintext = "SELECT * FROM users WHERE id=1"
        token = crypto.encrypt_field(plaintext)
        # Token is a Fernet base64 string, distinct from the plaintext
        assert token != plaintext
        assert isinstance(token, str)
        assert crypto.decrypt_field(token) == plaintext

    def test_unicode_payload(self, with_fernet_key: str) -> None:
        plaintext = "ünicode — testing 🚀"
        token = crypto.encrypt_field(plaintext)
        assert crypto.decrypt_field(token) == plaintext

    def test_empty_string_short_circuits(self, with_fernet_key: str) -> None:
        assert crypto.encrypt_field("") == ""
        assert crypto.decrypt_field("") == ""

    def test_decrypt_passes_through_non_fernet_token(
        self, with_fernet_key: str
    ) -> None:
        # Legacy plaintext stored before encryption was enabled should be
        # returned as-is rather than failing the audit reader.
        legacy = "this is not a fernet token"
        assert crypto.decrypt_field(legacy) == legacy

    def test_is_encryption_available_true(self, with_fernet_key: str) -> None:
        assert crypto.is_encryption_available() is True


# ---------------------------------------------------------------------------
# Without encryption configured
# ---------------------------------------------------------------------------


class TestWithoutFernetKey:
    def test_encrypt_returns_plaintext(self, without_fernet_key: None) -> None:
        assert crypto.encrypt_field("hello") == "hello"

    def test_decrypt_returns_input(self, without_fernet_key: None) -> None:
        assert crypto.decrypt_field("hello") == "hello"

    def test_is_encryption_available_false(self, without_fernet_key: None) -> None:
        assert crypto.is_encryption_available() is False


# ---------------------------------------------------------------------------
# Lazy-init protection
# ---------------------------------------------------------------------------


class TestLazyInit:
    def test_fernet_loaded_only_once(self, with_fernet_key: str) -> None:
        crypto.encrypt_field("first call")
        first_instance = crypto._fernet_instance
        crypto.encrypt_field("second call")
        assert crypto._fernet_instance is first_instance, (
            "Fernet object must be cached after first load"
        )

    def test_no_settings_object_falls_back_to_plaintext(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate import failure: deleting the attribute makes the
        # ``from … import settings`` line raise ImportError inside _get_fernet.
        import supertable.config.settings as settings_module

        monkeypatch.delattr(settings_module, "settings", raising=False)
        assert crypto.encrypt_field("plain") == "plain"
        assert crypto.decrypt_field("plain") == "plain"
        assert crypto.is_encryption_available() is False
