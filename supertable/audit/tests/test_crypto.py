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

import builtins
import json
from types import SimpleNamespace

import pytest

from cryptography.fernet import Fernet

from supertable.audit import crypto


@pytest.fixture(autouse=True)
def reset_crypto_module_state() -> None:
    """Force every test to re-initialize the lazy module-level cache."""
    crypto._fernet_instance = None
    crypto._fernet_loaded = False
    crypto._fernet_key = None
    yield
    crypto._fernet_instance = None
    crypto._fernet_loaded = False
    crypto._fernet_key = None


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

    def test_decrypt_rejects_non_fernet_token(
        self, with_fernet_key: str
    ) -> None:
        legacy = "this is not a fernet token"
        assert crypto.decrypt_field(legacy) is None

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

    def test_no_settings_object_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate import failure: deleting the attribute makes the
        # ``from … import settings`` line raise ImportError inside _get_fernet.
        import supertable.config.settings as settings_module

        monkeypatch.delattr(settings_module, "settings", raising=False)
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="configuration is unavailable",
        ):
            crypto.encrypt_field("plain")


class TestConfiguredEncryptionFailures:
    def test_missing_cryptography_dependency_fails_closed(
        self,
        with_fernet_key: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        real_import = builtins.__import__

        def missing_cryptography(name, *args, **kwargs):
            if name == "cryptography.fernet":
                raise ImportError("simulated missing cryptography")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", missing_cryptography)

        with pytest.raises(
            crypto.AuditEncryptionError,
            match="cryptography is required",
        ):
            crypto.protect_sensitive_detail(
                {"sql": "must-not-be-returned-as-plaintext"},
                action="query_execute",
            )
        assert crypto._fernet_loaded is False

    def test_invalid_configured_key_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import supertable.config.settings as settings_module

        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY="not-a-fernet-key"),
            raising=True,
        )

        with pytest.raises(
            crypto.AuditEncryptionError,
            match="failed to initialize configured audit encryption",
        ):
            crypto.encrypt_field("must-not-be-returned-as-plaintext")
        assert crypto._fernet_loaded is False

    def test_runtime_encryption_error_fails_closed(
        self, with_fernet_key: str
    ) -> None:
        class BrokenFernet:
            def encrypt(self, _value: bytes) -> bytes:
                raise RuntimeError("simulated encryption failure")

        crypto._fernet_instance = BrokenFernet()
        crypto._fernet_loaded = True
        crypto._fernet_key = with_fernet_key

        with pytest.raises(
            crypto.AuditEncryptionError,
            match="configured audit encryption failed",
        ):
            crypto.encrypt_field("must-not-be-returned-as-plaintext")


class TestDetailProtectionBudgets:
    @pytest.mark.parametrize(
        "detail",
        [
            [{"sql": "SELECT list_secret"}],
            ({"statement": "DELETE tuple_secret"},),
            '[{"nested":{"query_text":"SELECT json_array_secret"}}]',
        ],
    )
    def test_root_containers_protect_nested_sensitive_fields(
        self,
        without_fernet_key: None,
        detail: object,
    ) -> None:
        protected = crypto.protect_sensitive_detail(
            detail, action="data_write",
        )
        rendered = json.dumps(protected, default=str)
        assert "list_secret" not in rendered
        assert "tuple_secret" not in rendered
        assert "json_array_secret" not in rendered
        assert "_redacted" in rendered

    @pytest.mark.parametrize(
        "reserved_name",
        [
            "sql_encrypted",
            "SQL_SHA256",
            "query_text_redacted",
            "statement_encrypted",
        ],
    )
    def test_reserved_output_names_are_rejected_without_raw_field(
        self,
        without_fernet_key: None,
        reserved_name: str,
    ) -> None:
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="reserved sensitive-field output names",
        ):
            crypto.protect_sensitive_detail(
                {"nested": {reserved_name: "plaintext masquerade"}},
                action="data_write",
            )

    def test_container_item_limit_fails_before_transformation(
        self, without_fernet_key: None
    ) -> None:
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="container item limit",
        ):
            crypto.protect_sensitive_detail(
                {"items": list(range(crypto._MAX_DETAIL_CONTAINER_ITEMS + 1))},
                action="data_write",
            )

    def test_node_limit_fails_before_transformation(
        self, without_fernet_key: None
    ) -> None:
        payload = {
            f"group_{index}": [1, 2, 3, 4]
            for index in range(crypto._MAX_DETAIL_CONTAINER_ITEMS)
        }
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="structural node limit",
        ):
            crypto.protect_sensitive_detail(payload, action="data_write")

    def test_string_byte_limit_fails_before_encryption(
        self, without_fernet_key: None
    ) -> None:
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="string byte limit",
        ):
            crypto.protect_sensitive_detail(
                "x" * (crypto._MAX_DETAIL_STRING_BYTES + 1),
                action="query_execute",
            )

    def test_total_serialized_byte_limit_rejects_many_valid_strings(
        self, without_fernet_key: None
    ) -> None:
        payload = {
            "one": "x" * 24_000,
            "two": "y" * 24_000,
            "three": "z" * 24_000,
        }
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="byte limit",
        ):
            crypto.protect_sensitive_detail(payload, action="data_write")

    @pytest.mark.parametrize(
        "value, message",
        [
            (1 << (crypto._MAX_DETAIL_INTEGER_BITS + 1), "numeric limit"),
            (float("nan"), "non-finite"),
            (float("inf"), "non-finite"),
        ],
    )
    def test_numeric_limits_fail_before_serialization(
        self,
        without_fernet_key: None,
        value: object,
        message: str,
    ) -> None:
        with pytest.raises(crypto.AuditEncryptionError, match=message):
            crypto.protect_sensitive_detail(
                {"value": value}, action="data_write",
            )

    def test_configured_key_change_replaces_cached_fernet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import supertable.config.settings as settings_module

        key_one = Fernet.generate_key().decode("utf-8")
        key_two = Fernet.generate_key().decode("utf-8")
        mutable_settings = SimpleNamespace(
            SUPERTABLE_AUDIT_FERNET_KEY=key_one,
        )
        monkeypatch.setattr(
            settings_module, "settings", mutable_settings, raising=True,
        )
        first = crypto.encrypt_field("first")
        mutable_settings.SUPERTABLE_AUDIT_FERNET_KEY = key_two
        second = crypto.encrypt_field("second")

        assert Fernet(key_one.encode()).decrypt(first.encode()) == b"first"
        assert Fernet(key_two.encode()).decrypt(second.encode()) == b"second"
        assert crypto.decrypt_field(first) is None
