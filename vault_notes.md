# Connector Vault – Redis Keys & Storage

## Redis key patterns (per SuperTable)

Given:
- org = `<org>`
- sup = `<sup>`
- secret name = `<name>` (stored as a safe slug)

### 1) Meta index (Redis Set)
This set contains the list of secret names that exist for a SuperTable:

`supertable:<org>:<sup>:meta:vault:meta`

### 2) Secret entry (Redis String)
Each secret is stored as a single Redis String at:

`supertable:<org>:<sup>:meta:vault:<safe_slug(name)>`

`safe_slug(name)` replaces non `[a-zA-Z0-9_.-]` characters with `_`.

## Value format stored in Redis (JSON)

The secret entry value is JSON. The sensitive value is **never stored in plaintext**.

Example structure (fields may be empty if not supplied):

- `name`: original name you used
- `note`: optional note
- `enc`: `"fernet"`
- `ciphertext`: Fernet-encrypted value (string)
- `created_at_ms` / `updated_at_ms`: epoch millis
- `created_by` / `updated_by`: session user hash (if available)
- `redis_key`: the item key used
- `meta_key`: the meta set key used

## Encryption configuration

The Vault uses `cryptography.fernet` to encrypt/decrypt.

You must provide one of:
- `SUPERTABLE_VAULT_FERNET_KEY` (preferred)  
  - A standard Fernet key (urlsafe base64, 32 bytes)
- `SUPERTABLE_VAULT_MASTER_KEY`  
  - Any string; the app derives a Fernet key from SHA-256(master)

Best-effort fallbacks (only if MASTER key not set):
- `SUPERTABLE_SECRET_KEY`
- `SUPERTABLE_AUTH_SECRET`
- `SUPERTABLE_JWT_SECRET`

## API endpoints

All Vault endpoints are scoped to a SuperTable (org + sup):

- `GET  /reflection/connectors/vault/list?org=...&sup=...`
  - Returns `meta_key` and a list of secrets **without** ciphertext/value.

- `POST /reflection/connectors/vault/upsert`
  - Body: `{ org, sup, name, value, note?, user_hash? }`
  - Stores JSON with `ciphertext` in Redis and updates the meta set.

- `POST /reflection/connectors/vault/reveal`
  - Body: `{ org, sup, name }`
  - Returns `{ value }` (plaintext) for admins only.

- `DELETE /reflection/connectors/vault/<name>?org=...&sup=...`
  - Removes the secret entry and its name from the meta set.

## UI mapping

Connectors page tabs:
- **Source** – shows PyAirbyte (source) + Airbyte “New Source” + Saved Sources
- **Destination** – shows PyAirbyte (destination) + Airbyte “New Destination” + Saved Destinations
- **Vault** – shows the Vault list/editor (encrypted storage in Redis)
