#!/usr/bin/env sh
# test_entrypoint.sh — unit tests for entrypoint.sh logic
# Run with:  sh test_entrypoint.sh
# No Docker, no pytest, no dependencies — pure POSIX sh.
#
# Strategy: source a "testable" version of the entrypoint with the exec/python
# calls replaced by stubs, then assert on env vars and exit codes.

set -eu

PASS=0
FAIL=0
ENTRYPOINT="${1:-$(dirname "$0")/entrypoint.sh}"

# ---------------------------------------------------------------------------
# Tiny test framework
# ---------------------------------------------------------------------------
_pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS + 1)); }
_fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAIL=$((FAIL + 1)); }

assert_eq() {
  desc="$1"; expected="$2"; actual="$3"
  if [ "$actual" = "$expected" ]; then
    _pass "$desc"
  else
    _fail "$desc — expected '$expected', got '$actual'"
  fi
}

assert_exit() {
  desc="$1"; expected_code="$2"
  shift 2
  set +e
  "$@" >/dev/null 2>&1
  actual_code=$?
  set -e
  if [ "$actual_code" = "$expected_code" ]; then
    _pass "$desc"
  else
    _fail "$desc — expected exit $expected_code, got $actual_code"
  fi
}

# ---------------------------------------------------------------------------
# Build a stub entrypoint: replace exec/python lines with env-dumping stubs.
# The stub writes key env vars to a temp file and exits 0.
# ---------------------------------------------------------------------------
STUB=$(mktemp /tmp/ep_stub.XXXXXX.sh)
ENVDUMP=$(mktemp /tmp/ep_envdump.XXXXXX)

# Replace every `exec python ...` and `exec uvicorn ...` line with a stub that
# dumps the environment and exits cleanly.
sed \
  -e 's|exec python .*|env > '"$ENVDUMP"'; exit 0|g' \
  -e 's|exec uvicorn .*|env > '"$ENVDUMP"'; exit 0|g' \
  -e 's|python -u /app/supertable/mcp/web_server.py &|true \&|g' \
  "$ENTRYPOINT" > "$STUB"
chmod +x "$STUB"

_env_val() {
  # Read a key from the last envdump
  grep "^${1}=" "$ENVDUMP" 2>/dev/null | head -1 | cut -d= -f2- || echo ""
}

_run_stub() {
  # Run the stub with given env overrides, capture envdump
  rm -f "$ENVDUMP"
  env -i HOME=/tmp/test_home PATH="$PATH" "$@" sh "$STUB" >/dev/null 2>&1 || true
}

# ---------------------------------------------------------------------------
# 1. Default HOME fallback
# ---------------------------------------------------------------------------
printf '\n--- 1. HOME fallback ---\n'

_run_stub SERVICE=reflection
got=$(_env_val HOME)
# HOME was explicitly set to /tmp/test_home via env -i
assert_eq "HOME preserved when set" "/tmp/test_home" "$got"

# ---------------------------------------------------------------------------
# 2. DUCKDB_EXTENSION_DIRECTORY default
# ---------------------------------------------------------------------------
printf '\n--- 2. DUCKDB_EXTENSION_DIRECTORY ---\n'

_run_stub SERVICE=reflection HOME=/tmp/test_home
got=$(_env_val DUCKDB_EXTENSION_DIRECTORY)
assert_eq "Default DUCKDB dir under HOME" "/tmp/test_home/.duckdb/extensions" "$got"

CUSTOM_DUCKDB=$(mktemp -d /tmp/custom_duckdb.XXXXXX)
_run_stub SERVICE=reflection HOME=/tmp/test_home DUCKDB_EXTENSION_DIRECTORY="$CUSTOM_DUCKDB"
got=$(_env_val DUCKDB_EXTENSION_DIRECTORY)
assert_eq "Explicit DUCKDB dir respected" "$CUSTOM_DUCKDB" "$got"
rm -rf "$CUSTOM_DUCKDB"

# ---------------------------------------------------------------------------
# 3. Tilde expansion in DUCKDB_EXTENSION_DIRECTORY
# ---------------------------------------------------------------------------
printf '\n--- 3. Tilde expansion ---\n'

_run_stub SERVICE=reflection HOME=/tmp/test_home DUCKDB_EXTENSION_DIRECTORY="~/.duckdb/extensions"
got=$(_env_val DUCKDB_EXTENSION_DIRECTORY)
assert_eq "Tilde expanded to HOME" "/tmp/test_home/.duckdb/extensions" "$got"

_run_stub SERVICE=reflection HOME=/tmp/test_home DUCKDB_EXTENSION_DIRECTORY="~/custom/ext"
got=$(_env_val DUCKDB_EXTENSION_DIRECTORY)
assert_eq "Tilde + subpath expanded" "/tmp/test_home/custom/ext" "$got"

# ---------------------------------------------------------------------------
# 4. SERVICE default
# ---------------------------------------------------------------------------
printf '\n--- 4. SERVICE default ---\n'

# When SERVICE is not set, the stub should still exit 0 (dispatches to reflection)
rm -f "$ENVDUMP"
env -i HOME=/tmp/test_home PATH="$PATH" sh "$STUB" >/dev/null 2>&1 || true
if [ -f "$ENVDUMP" ]; then
  _pass "Default SERVICE (reflection) dispatched successfully"
else
  _fail "Default SERVICE did not dispatch"
fi

# ---------------------------------------------------------------------------
# 5. mcp-http sets SUPERTABLE_MCP_TRANSPORT=streamable-http
# ---------------------------------------------------------------------------
printf '\n--- 5. mcp-http transport env ---\n'

_run_stub SERVICE=mcp-http HOME=/tmp/test_home
got=$(_env_val SUPERTABLE_MCP_TRANSPORT)
assert_eq "mcp-http sets SUPERTABLE_MCP_TRANSPORT" "streamable-http" "$got"

# mcp stdio should NOT set the transport var
_run_stub SERVICE=reflection HOME=/tmp/test_home
got=$(_env_val SUPERTABLE_MCP_TRANSPORT)
assert_eq "reflection does not set SUPERTABLE_MCP_TRANSPORT" "" "$got"

# ---------------------------------------------------------------------------
# 6. Unknown SERVICE exits 64
# ---------------------------------------------------------------------------
printf '\n--- 6. Unknown SERVICE exit code ---\n'

assert_exit "Unknown SERVICE exits 64" 64 \
  env -i HOME=/tmp/test_home PATH="$PATH" SERVICE=bogus sh "$ENTRYPOINT"

assert_exit "Explicitly invalid SERVICE exits 64" 64 \
  env -i HOME=/tmp/test_home PATH="$PATH" SERVICE=INVALID sh "$ENTRYPOINT"

# ---------------------------------------------------------------------------
# 7. Runtime dirs are created
# ---------------------------------------------------------------------------
printf '\n--- 7. Runtime dir creation ---\n'

TMPBASE=$(mktemp -d /tmp/ep_dirs.XXXXXX)
_run_stub SERVICE=reflection HOME="$TMPBASE"

if [ -d "${TMPBASE}/.duckdb/extensions" ]; then
  _pass "DUCKDB extension dir created"
else
  _fail "DUCKDB extension dir not created"
fi

if [ -d "${TMPBASE}/supertable" ]; then
  _pass "supertable work dir created"
else
  _fail "supertable work dir not created"
fi

rm -rf "$TMPBASE"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
rm -f "$STUB" "$ENVDUMP"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
printf '\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'
printf '  Results: \033[32m%d passed\033[0m, \033[31m%d failed\033[0m\n' "$PASS" "$FAIL"
printf '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'

[ "$FAIL" -eq 0 ]