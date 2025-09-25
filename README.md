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
    - [Authentication methods](#authentication-methods)
    - [Using abfss:// locations](#using-abfss-locations)
    - [Run from Azure Synapse (Managed Identity)](#run-from-azure-synapse-managed-identity)
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

---

## Quick Start

```bash
# Show help
supertable -h

# Example: S3 + Redis → write .env
supertable --storage S3 --write .env   --aws-access-key-id AKIA...   --aws-secret-access-key "...secret..."   --aws-region eu-central-1   --redis-url redis://:password@redis:6379/0

# Example: LOCAL (project folder) → write .env
supertable --storage LOCAL --write .env   --local-home "$HOME/supertable" --create-local-home
```

---

## Configuration via CLI

`supertable` initializes and (optionally) validates environment variables for **LOCAL**, **S3**, **MINIO**, **AZURE**, and **GCP**, plus **Redis** (used for locking in non-LOCAL modes; optional for LOCAL).

- `--write .env` writes variables to a file  
- `--write -` prints `export` lines to stdout (pipe into your shell)  
- Validation runs by default; use `--no-validate` if services aren’t reachable during setup

### Local filesystem (LOCAL)

```bash
supertable --storage LOCAL   --local-home "$HOME/supertable"   --create-local-home   --write .env
```

### Amazon S3

```bash
supertable --storage S3   --aws-access-key-id AKIA...   --aws-secret-access-key "...secret..."   --aws-region eu-central-1   --redis-url redis://:password@redis:6379/0   --write .env
```

### MinIO (S3-compatible)

```bash
supertable --storage MINIO   --aws-access-key-id minioadmin   --aws-secret-access-key minioadmin   --aws-region us-east-1   --aws-endpoint-url http://localhost:9000   --aws-force-path-style true   --redis-url redis://:password@localhost:6379/0   --no-validate   --write .env
```

### Azure Blob Storage

#### Authentication methods

**Priority order (first match wins):**
1. Connection String  
2. Account Key  
3. SAS Token  
4. Managed Identity / AAD  

##### Managed Identity (default if no secrets provided)
```bash
supertable --storage AZURE   --home "abfss://<container>@<account>.dfs.core.windows.net/<prefix>"   --write .env
```

##### Account Key (overrides MI)
```bash
supertable --storage AZURE   --home "abfss://<container>@<account>.dfs.core.windows.net/<prefix>"   --azure-key "<ACCOUNT_KEY>"   --write .env
```

##### SAS token (overrides MI)
```bash
supertable --storage AZURE   --home "abfss://<container>@<account>.dfs.core.windows.net/<prefix>"   --azure-sas "?sv=..."   --write .env
```

##### Connection String (highest priority)
```bash
supertable --storage AZURE   --azure-connection-string "DefaultEndpointsProtocol=...;AccountName=<account>;AccountKey=...;EndpointSuffix=core.windows.net"   --write .env
```

##### Forcing MI explicitly
Do not set any of: `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_KEY`, `AZURE_SAS_TOKEN`.  

#### Using abfss:// locations

Format:
```
abfss://{container}@{account}.dfs.core.windows.net/{prefix}
```

#### Run from Azure Synapse (Managed Identity)

```python
!pip install "supertable[azure]"

!supertable --storage AZURE   --home "abfss://storage@kladnasoft.dfs.core.windows.net/supertable"   --write .env

from dotenv import load_dotenv
load_dotenv(".env")

from supertable.super_table import SuperTable
st = SuperTable(super_name="demo-new")
```

### Google Cloud Storage (GCP)

```bash
supertable --storage GCP   --gcp-project my-gcp-project   --gcp-credentials /path/to/sa.json   --redis-url redis://:password@redis:6379/0   --write .env
```

---

## Key Features

- Automatic table creation
- Self-referencing architecture
- Staging module with history
- Columnar storage
- Built-in RBAC
- Platform independent

---

## Examples

See `examples/` folder for usage demos.

---

## Benefits

- Quick start
- Higher efficiency
- Holistic insights
- Cost savings
