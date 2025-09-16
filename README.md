# SuperTable

![Python](https://img.shields.io/badge/python-3.10+-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**  
A high-performance, lightweight transaction catalog that integrates multiple
basic tables into a single, cohesive framework designed for ultimate
efficiency.
It automatically creates and manages tables so you can start running SQL queries
immediately—no complicated schemas or manual joins required.

---

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration via CLI](#configuration-via-cli)
  - [Local filesystem (LOCAL)](#local-filesystem-local)
  - [Amazon S3](#amazon-s3)
  - [MinIO (S3-compatible)](#minio-s3-compatible)
  - [Azure Blob Storage](#azure-blob-storage)
  - [Google Cloud Storage (GCP)](#google-cloud-storage-gcp)
  - [Validation behavior](#validation-behavior)
  - [Cheat sheet](#cheat-sheet-required-env-by-backend)
- [Setup](#setup)
- [Key Features](#key-features)
- [Examples](#examples)
- [Benefits](#benefits)

---

## Installation

```bash
# Core (LOCAL only)
pip install supertable

# AWS S3 (installs boto3 + redis)
pip install "supertable[s3]"

# MinIO (uses AWS-style SDK + redis)
pip install "supertable[minio]"

# Azure Blob Storage (installs azure-storage-blob + redis)
pip install "supertable[azure]"

# Google Cloud Storage (installs google-cloud-storage + redis)
pip install "supertable[gcp]"

# Everything (all cloud backends + redis)
pip install "supertable[all-cloud]"
```

> The CLI is installed as `supertable`. It writes a `.env` your app can load.

---

## Quick Start

```bash
# Show help
supertable config -h

# Example: S3 + Redis → write .env
supertable config --storage S3 --write .env \
  --aws-access-key-id AKIA... \
  --aws-secret-access-key "...secret..." \
  --aws-region eu-central-1 \
  --redis-url redis://:password@redis:6379/0

# Example: LOCAL (project folder) → write .env
supertable config --storage LOCAL --write .env \
  --local-home "$HOME/supertable" --create-local-home
```

---

## Configuration via CLI

`supertable config` initializes and (optionally) validates environment variables for **LOCAL**, **S3**, **MINIO**, **AZURE**, and **GCP**, plus **Redis** (used for locking in non-LOCAL modes; optional for LOCAL).

- `--write .env` writes variables to a file  
- `--write -` prints `export` lines to stdout (pipe into your shell)  
- Validation runs by default; use `--no-validate` if services aren’t reachable during setup

### Local filesystem (LOCAL)

**No Redis (default):**
```bash
supertable config --storage LOCAL \
  --local-home "$HOME/supertable" \
  --create-local-home \
  --write .env
```

Writes:
```dotenv
STORAGE_TYPE=LOCAL
SUPERTABLE_HOME=/home/you/supertable
```

**With Redis while LOCAL (optional):**
```bash
supertable config --storage LOCAL \
  --local-home "$HOME/supertable" \
  --redis-with-local \
  --redis-url redis://localhost:6379/0 \
  --write .env
```

### Amazon S3

```bash
supertable config --storage S3 \
  --aws-access-key-id AKIA... \
  --aws-secret-access-key "...secret..." \
  --aws-region eu-central-1 \
  --redis-url redis://:password@redis:6379/0 \
  --write .env
```

Writes:
```dotenv
STORAGE_TYPE=S3
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...secret...
AWS_DEFAULT_REGION=eu-central-1
REDIS_URL=redis://:password@redis:6379/0
```

### MinIO (S3-compatible)

MinIO uses AWS-style credentials + a custom endpoint and often **path-style**:

```bash
supertable config --storage MINIO \
  --aws-access-key-id minioadmin \
  --aws-secret-access-key minioadmin \
  --aws-region us-east-1 \
  --aws-endpoint-url http://localhost:9000 \
  --aws-force-path-style true \
  --redis-url redis://:password@localhost:6379/0 \
  --no-validate \
  --write .env
```

Writes:
```dotenv
STORAGE_TYPE=MINIO
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=us-east-1
AWS_S3_ENDPOINT_URL=http://localhost:9000
AWS_S3_FORCE_PATH_STYLE=true
REDIS_URL=redis://:password@localhost:6379/0
```

### Azure Blob Storage

Pick one auth mode (Account Key, SAS, Connection String, or AAD/Managed Identity).

**Account Key example:**
```bash
supertable config --storage AZURE \
  --azure-account myaccount \
  --azure-key "xxxxxxxxxxxxxxxx" \
  --azure-endpoint https://myaccount.blob.core.windows.net \
  --redis-url redis://:password@redis:6379/0 \
  --write .env
```

Writes:
```dotenv
STORAGE_TYPE=AZURE
AZURE_STORAGE_ACCOUNT=myaccount
AZURE_STORAGE_KEY=xxxxxxxxxxxxxxxx
AZURE_BLOB_ENDPOINT=https://myaccount.blob.core.windows.net
REDIS_URL=redis://:password@redis:6379/0
```

**Other Azure variants**
- SAS token: `--azure-sas "?sv=..."`  
- Connection string: `--azure-connection-string "DefaultEndpointsProtocol=..."`  
- AAD/Managed Identity: provide `--azure-account` and `--azure-endpoint`; credentials are resolved by the environment (e.g., VM/AKS)

### Google Cloud Storage (GCP)

**With service-account key file:**
```bash
supertable config --storage GCP \
  --gcp-project my-gcp-project \
  --gcp-credentials /path/to/sa.json \
  --redis-url redis://:password@redis:6379/0 \
  --write .env
```

**Using ADC (Application Default Credentials):**
```bash
supertable config --storage GCP \
  --gcp-project my-gcp-project \
  --redis-url redis://:password@redis:6379/0 \
  --write .env
```

Writes:
```dotenv
STORAGE_TYPE=GCP
GCP_PROJECT=my-gcp-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json   # if provided
REDIS_URL=redis://:password@redis:6379/0
```

> You can also pass raw JSON to `--gcp-credentials`; the CLI validates it without needing a temp file.

### Validation behavior

- **Redis:** `PING` check  
- **S3 (AWS):** STS identity check  
- **MinIO:** `s3.list_buckets()` when `--aws-endpoint-url` is set; otherwise use `--no-validate`  
- **Azure:** `get_service_properties()` using your chosen auth  
- **GCP:** attempts to list one bucket to verify credentials/project

Use `--no-validate` when CI/network cannot reach the services during setup.

### Cheat sheet (required env by backend)

| STORAGE_TYPE | Required Vars (typical) |
|---|---|
| LOCAL | `SUPERTABLE_HOME` |
| S3 | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` |
| MINIO | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_S3_ENDPOINT_URL`, `AWS_S3_FORCE_PATH_STYLE=true` |
| AZURE | One of: **(a)** `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_KEY` (+ optional `AZURE_BLOB_ENDPOINT`), **(b)** `AZURE_SAS_TOKEN`, **(c)** `AZURE_STORAGE_CONNECTION_STRING`, **(d)** AAD/Managed Identity (`AZURE_STORAGE_ACCOUNT` + `AZURE_BLOB_ENDPOINT`) |
| GCP | `GOOGLE_APPLICATION_CREDENTIALS` **or** raw JSON via `GCP_SA_JSON`, plus optional `GCP_PROJECT` |

> For all **non-LOCAL** backends, Redis is recommended for locking. Provide **either** `REDIS_URL` **or** `REDIS_HOST`/`REDIS_PORT`/`REDIS_DB`/`REDIS_PASSWORD`/`REDIS_SSL`.

---

---

## Key Features

- **Automatic table creation**  
  Load your data and SuperTable instantly builds the required tables and
  columns—no predefined schema or extra setup.

- **Self-referencing architecture**  
  Combine and analyze data across tables without writing manual joins.

- **Staging module with history**  
  Upload files to a staging area and reload any version at any time, keeping a
  complete audit trail for tracking and compliance.

- **Columnar storage for speed**  
  With fully denormalized columnar storage, queries remain lightning-fast, 
  even when dealing with thousands of columns.

- **Built-in RBAC security**  
  Define users and roles to control row- and column-level access—no external
  security tools required.

- **Platform independent**  
  Deploy on any major cloud provider or on-premise. SuperTable is a pure Python
  library with no hidden costs.

---

## Examples

The project ships with an **`examples/`** folder that walks you through common
workflows:

| Script prefix | What it shows |
|---------------|---------------|
| **1.\*** | Create a SuperTable, roles, and users |
| **2.\*** | Write dummy or single-file data into a simple table |
| **3.\*** | Read data, query statistics, and inspect metadata |
| **4.1**   | Clean obsolete files |
| **5.\*** | Delete tables and supertables |

Additional utility scripts demonstrate locking, parallel writes, and
performance measurement. Browse the folder to get started quickly.

---

## Benefits

- **Quick start**  
  Go from raw data to query-ready in minutes—faster than spreadsheets or
  traditional databases.

- **Higher efficiency**  
  Eliminate manual steps and rework so you spend more time analyzing and less
  time wrangling.

- **Holistic insights**  
  Analyze datasets individually or together to uncover trends, outliers, and
  cross-dependencies.

- **Cost savings**  
  Consolidate licenses, simplify support, and reinvest the savings in deeper
  analytics.

---
## 🚀 SuperTable Benchmark (Serialized Run)

Our latest benchmark shows how **SuperTable** performs in a serialized run on real data:

<table>
<tr>
<td valign="top">

<h3>💡 Performance Highlights</h3>

<ul>
  <li>👉 <b>29,788 files processed</b></li>
  <li>⚡ ~<b>18,865 rows/sec</b> throughput</li>
  <li>📂 ~<b>4.9 files/sec</b> processed</li>
  <li>📈 <b>1.1 MB/sec</b> sustained throughput</li>
  <li>✅ <b>74 million new rows inserted</b> with zero deletions</li>
</ul>

</td>
<td valign="top">

<img width="226" height="340" alt="SuperTable Benchmark" src="https://github.com/user-attachments/assets/b53ace69-098c-4953-b18a-460571e15da5" />

</td>
</tr>
</table>


---

SuperTable provides a flexible, high-performance solution that grows with your
business. Cut complexity, save time, and gain deeper insights—all in a single,
streamlined platform.
