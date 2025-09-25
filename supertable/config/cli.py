# supertable/config/cli.py
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

# -----------------------
# Helpers / lazy imports
# -----------------------


def try_import(name: str):
    try:
        return __import__(name)
    except Exception as e:
        print(
            f"[WARN] Optional dependency '{name}' is not installed: {e}",
            file=sys.stderr,
        )
        return None


def _norm_bool(val: str | bool) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def write_env_file(path: Path, kv: Dict[str, str]) -> None:
    lines = []
    for k, v in kv.items():
        v_str = "" if v is None else str(v)
        if any(ch in v_str for ch in [" ", "#", '"', "'"]):
            v_str = '"' + v_str.replace('"', '\\"') + '"'
        lines.append(f"{k}={v_str}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# -----------------------
# ABFSS parsing
# -----------------------


def parse_abfss(uri: str) -> Tuple[str, str, str, str]:
    """
    Parse an abfss:// URL of the form:
      abfss://{container}@{account}.dfs.core.windows.net/{prefix...}

    Returns: (account, container, blob_endpoint, prefix)
    Raises ValueError if malformed.
    """
    if not uri.lower().startswith("abfss://"):
        raise ValueError("ABFSS URI must start with 'abfss://'")

    # Strip scheme
    rest = uri[8:]

    # Split before first '/' to isolate authority, then path->prefix
    if "/" in rest:
        authority, prefix = rest.split("/", 1)
    else:
        authority, prefix = rest, ""

    if "@" not in authority or ".dfs.core.windows.net" not in authority:
        raise ValueError(f"Malformed ABFSS authority: {authority}")

    container, host = authority.split("@", 1)

    if not container:
        raise ValueError("Missing container in ABFSS URI")

    # host like: {account}.dfs.core.windows.net
    if not host.endswith(".dfs.core.windows.net"):
        raise ValueError("ABFSS host must end with '.dfs.core.windows.net'")

    account = host.split(".dfs.core.windows.net")[0]
    if "." in account or not account:
        # Guard against unexpected subdomains
        account = account.split(".", 1)[0]

    blob_endpoint = f"https://{account}.blob.core.windows.net"
    prefix = prefix.strip("/")

    return account, container, blob_endpoint, prefix


# -----------------------
# Validators
# -----------------------


def validate_redis(
    redis_url: Optional[str],
    host: str,
    port: int,
    db: int,
    password: Optional[str],
    ssl: bool,
) -> tuple[bool, str]:
    redis = try_import("redis")
    if redis is None:
        return False, "redis client not installed"
    try:
        client = (
            redis.from_url(redis_url)
            if redis_url
            else redis.Redis(
                host=host, port=port, db=db, password=(password or None), ssl=ssl
            )
        )
        ok = client.ping()
        return bool(ok), "OK" if ok else "PING returned False"
    except Exception as e:
        return False, f"Redis validation failed: {e}"


def _mk_boto3_config(force_path_style: bool):
    boto3 = try_import("boto3")
    if boto3 is None:
        return None
    # Some older botocore versions may not accept dict for s3 arg in Config.
    try:
        return boto3.session.Config(
            s3={"addressing_style": "path" if force_path_style else "virtual"}
        )
    except Exception:
        return boto3.session.Config(signature_version="s3v4")


def validate_s3_or_minio(
    aws_key: str,
    aws_secret: str,
    region: str,
    endpoint_url: str,
    force_path_style: bool,
    storage: str,
) -> tuple[bool, str]:
    boto3 = try_import("boto3")
    if boto3 is None:
        return False, "boto3 not installed"

    # Prefer STS for AWS; for MinIO (or any custom endpoint) fall back to S3 ListBuckets
    try:
        cfg = _mk_boto3_config(force_path_style)
        if endpoint_url or storage == "MINIO":
            s3 = boto3.client(
                "s3",
                aws_access_key_id=aws_key or None,
                aws_secret_access_key=aws_secret or None,
                region_name=region or None,
                endpoint_url=endpoint_url or None,
                config=cfg,
            )
            # cheap call; requires listing permission (MinIO root user has it)
            s3.list_buckets()
            return True, "OK (S3/MinIO list_buckets)"
        else:
            sts = boto3.client(
                "sts",
                aws_access_key_id=aws_key or None,
                aws_secret_access_key=aws_secret or None,
                region_name=region or None,
            )
            ident = sts.get_caller_identity()
            return True, f"OK (AWS STS: Account={ident.get('Account','?')})"
    except Exception as e:
        return False, f"S3/MinIO validation failed: {e}"


def validate_azure(
    account: str, key: str, conn_str: str, sas: str, endpoint: str
) -> tuple[bool, str]:
    az = try_import("azure.storage.blob")
    if az is None:
        return False, "azure-storage-blob not installed"
    try:
        from azure.storage.blob import BlobServiceClient

        client = None
        if conn_str:
            client = BlobServiceClient.from_connection_string(conn_str)
        else:
            url = endpoint or (
                f"https://{account}.blob.core.windows.net" if account else None
            )
            if not url:
                return False, "missing account or endpoint"
            if key:
                client = BlobServiceClient(account_url=url, credential=key)
            elif sas:
                if not sas.startswith("?"):
                    sas = "?" + sas
                client = BlobServiceClient(account_url=url + sas)
            else:
                ident = try_import("azure.identity")
                if not ident:
                    return False, "no credential provided (key/sas/connection string/AAD)"
                from azure.identity import DefaultAzureCredential

                client = BlobServiceClient(
                    account_url=url, credential=DefaultAzureCredential()
                )
        client.get_service_properties()
        return True, "OK"
    except Exception as e:
        return False, f"Azure validation failed: {e}"


def validate_gcp(project: str, creds_path: str) -> tuple[bool, str]:
    gcs = try_import("google.cloud.storage")
    if gcs is None:
        return False, "google-cloud-storage not installed"
    try:
        from google.cloud import storage

        client: storage.Client
        if creds_path:
            # Allow raw JSON content too (path OR JSON string)
            if Path(creds_path).exists():
                from google.oauth2 import service_account

                creds = service_account.Credentials.from_service_account_file(
                    creds_path
                )
                client = storage.Client(
                    project=project or creds.project_id, credentials=creds
                )
            else:
                # try parsing as JSON string
                info = json.loads(creds_path)
                from google.oauth2 import service_account

                creds = service_account.Credentials.from_service_account_info(info)
                client = storage.Client(
                    project=project or creds.project_id, credentials=creds
                )
        else:
            # ADC (env GOOGLE_APPLICATION_CREDENTIALS or metadata)
            client = storage.Client(project=project or None)

        # Cheap call; may require permissions. If unauthorized, give a helpful error.
        _ = next(client.list_buckets(page_size=1), None)
        return True, "OK"
    except Exception as e:
        return False, f"GCP validation failed: {e}"


# -----------------------
# CLI
# -----------------------


def _add_primary_args(p: argparse.ArgumentParser) -> None:
    """Primary (current) flags – kept for backward compatibility."""
    p.add_argument(
        "--storage",
        default="S3",
        choices=["LOCAL", "S3", "MINIO", "AZURE", "GCP"],
        help="Storage backend to configure (default: S3).",
    )
    p.add_argument(
        "--write",
        metavar="FILE",
        default=".env",
        help="Where to write env file (default: .env). Use '-' to print shell exports.",
    )
    p.add_argument(
        "--no-validate", action="store_true", help="Skip live connectivity checks."
    )
    # Workspace / home (optional). Accept ABFSS for Azure.
    p.add_argument(
        "--home",
        default=os.getenv("SUPERTABLE_HOME", ""),
        help=(
            "Workspace/home path (LOCAL dir or abfss:// for Azure). Writes "
            "SUPERTABLE_HOME. When abfss:// is provided, Azure settings are "
            "inferred automatically."
        ),
    )
    # LOCAL
    p.add_argument(
        "--local-home",
        default=os.getenv("SUPERTABLE_HOME", str(Path.home() / "supertable")),
        help="Local workspace path for STORAGE_TYPE=LOCAL; writes SUPERTABLE_HOME.",
    )
    p.add_argument(
        "--create-local-home",
        action="store_true",
        help="Create the --local-home directory if it doesn't exist.",
    )
    p.add_argument(
        "--redis-with-local",
        action="store_true",
        help="Also configure & validate Redis when STORAGE_TYPE=LOCAL (optional).",
    )
    # S3 / MinIO (AWS style)
    p.add_argument("--aws-access-key-id", default=os.getenv("AWS_ACCESS_KEY_ID", ""))
    p.add_argument(
        "--aws-secret-access-key", default=os.getenv("AWS_SECRET_ACCESS_KEY", "")
    )
    p.add_argument(
        "--aws-region", default=os.getenv("AWS_DEFAULT_REGION", "eu-central-1")
    )
    p.add_argument(
        "--aws-endpoint-url",
        default=os.getenv("AWS_S3_ENDPOINT_URL", ""),  # for MinIO/custom S3
        help="Custom S3 endpoint (e.g., http://localhost:9000 for MinIO).",
    )
    p.add_argument(
        "--aws-force-path-style",
        default=os.getenv("AWS_S3_FORCE_PATH_STYLE", "false"),
        help="true/false. For MinIO or path-style only setups.",
    )
    # Azure
    p.add_argument("--azure-account", default=os.getenv("AZURE_STORAGE_ACCOUNT", ""))
    p.add_argument("--azure-key", default=os.getenv("AZURE_STORAGE_KEY", ""))
    p.add_argument("--azure-sas", default=os.getenv("AZURE_SAS_TOKEN", ""))
    p.add_argument(
        "--azure-connection-string",
        default=os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
    )
    p.add_argument("--azure-endpoint", default=os.getenv("AZURE_BLOB_ENDPOINT", ""))
    p.add_argument("--azure-container", default=os.getenv("AZURE_CONTAINER", ""))
    p.add_argument(
        "--azure-prefix",
        default=os.getenv("SUPERTABLE_PREFIX", ""),
        help="Optional base prefix for object keys (used to scope all paths).",
    )
    # GCP
    p.add_argument("--gcp-project", default=os.getenv("GCP_PROJECT", ""))
    p.add_argument(
        "--gcp-credentials",
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
        help="Path to service-account JSON, or raw JSON string. If empty, uses ADC.",
    )
    # Redis (locking for non-LOCAL; optional for LOCAL via --redis-with-local)
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", ""))
    p.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    p.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    p.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", "0")))
    p.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD", ""))
    p.add_argument(
        "--redis-ssl",
        action="store_true",
        default=_norm_bool(os.getenv("REDIS_SSL", "false")),
    )


def _add_cloud_companion_args(p: argparse.ArgumentParser) -> None:
    """
    Additional flags to declare a *secondary* cloud profile while keeping LOCAL
    as primary storage. This matches the "local disk + cloud services" workflow.
    Variables are emitted with the 'CLOUD_' prefix (e.g., CLOUD_STORAGE_TYPE).
    """
    p.add_argument(
        "--also-cloud",
        choices=["S3", "MINIO", "AZURE", "GCP"],
        default=os.getenv("CLOUD_STORAGE_TYPE", "") or None,
        help=(
            "Optionally configure a secondary cloud backend alongside LOCAL. "
            "Emits CLOUD_* env vars for your app to consume."
        ),
    )

    # S3 / MinIO for CLOUD_
    p.add_argument(
        "--cloud-aws-access-key-id", default=os.getenv("CLOUD_AWS_ACCESS_KEY_ID", "")
    )
    p.add_argument(
        "--cloud-aws-secret-access-key",
        default=os.getenv("CLOUD_AWS_SECRET_ACCESS_KEY", ""),
    )
    p.add_argument(
        "--cloud-aws-region",
        default=os.getenv("CLOUD_AWS_DEFAULT_REGION", "eu-central-1"),
    )
    p.add_argument(
        "--cloud-aws-endpoint-url",
        default=os.getenv("CLOUD_AWS_S3_ENDPOINT_URL", ""),
        help="Custom S3 endpoint for CLOUD_ (e.g., http://localhost:9000 for MinIO).",
    )
    p.add_argument(
        "--cloud-aws-force-path-style",
        default=os.getenv("CLOUD_AWS_S3_FORCE_PATH_STYLE", "false"),
        help="true/false. For MinIO/path-style addressing (CLOUD_).",
    )

    # Azure for CLOUD_
    p.add_argument(
        "--cloud-azure-account", default=os.getenv("CLOUD_AZURE_STORAGE_ACCOUNT", "")
    )
    p.add_argument("--cloud-azure-key", default=os.getenv("CLOUD_AZURE_STORAGE_KEY", ""))
    p.add_argument("--cloud-azure-sas", default=os.getenv("CLOUD_AZURE_SAS_TOKEN", ""))
    p.add_argument(
        "--cloud-azure-connection-string",
        default=os.getenv("CLOUD_AZURE_STORAGE_CONNECTION_STRING", ""),
    )
    p.add_argument(
        "--cloud-azure-endpoint", default=os.getenv("CLOUD_AZURE_BLOB_ENDPOINT", "")
    )
    p.add_argument(
        "--cloud-azure-container", default=os.getenv("CLOUD_AZURE_CONTAINER", "")
    )
    p.add_argument(
        "--cloud-azure-prefix",
        default=os.getenv("CLOUD_SUPERTABLE_PREFIX", ""),
        help="Optional base prefix for CLOUD_ object keys.",
    )

    # GCP for CLOUD_
    p.add_argument("--cloud-gcp-project", default=os.getenv("CLOUD_GCP_PROJECT", ""))
    p.add_argument(
        "--cloud-gcp-credentials",
        default=os.getenv("CLOUD_GOOGLE_APPLICATION_CREDENTIALS", ""),
        help=(
            "Path to service-account JSON, or raw JSON string for CLOUD_. "
            "If empty, uses ADC."
        ),
    )


def _maybe_parse_abfss_for_azure_home(
    args: argparse.Namespace, kv: Dict[str, str], prefix: str = ""
) -> None:
    # If --home is abfss://..., infer account/container/endpoint/prefix automatically
    if args.home and args.home.lower().startswith("abfss://"):
        try:
            account, container, endpoint, abfss_prefix = parse_abfss(args.home)
            kv.setdefault(prefix + "AZURE_STORAGE_ACCOUNT", account)
            kv.setdefault(prefix + "AZURE_BLOB_ENDPOINT", endpoint)
            kv.setdefault(prefix + "AZURE_CONTAINER", container)
            if abfss_prefix:
                kv.setdefault(prefix + "SUPERTABLE_PREFIX", abfss_prefix)
            print(
                f"[INFO] Parsed ABFSS: account={account}, container={container}, "
                f"endpoint={endpoint}, prefix='{abfss_prefix}'"
            )
        except ValueError as ve:
            print(f"[WARN] Could not parse ABFSS home: {ve}", file=sys.stderr)


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="supertable",
        description=(
            "Initialize and validate SuperTable environment for LOCAL, S3, MINIO, "
            "AZURE, or GCP, plus Redis. Supports a secondary CLOUD_ profile to run "
            "on local disk while authenticating to cloud services."
        ),
    )
    _add_primary_args(p)
    _add_cloud_companion_args(p)

    args = p.parse_args(argv)
    storage = args.storage.upper()

    # What to include
    need_aws = storage in {"S3", "MINIO"}
    need_azure = storage == "AZURE"
    need_gcp = storage == "GCP"

    # If the user wants a local-first + cloud companion setup, we emit CLOUD_* too.
    also_cloud: Optional[str] = args.also_cloud.upper() if args.also_cloud else None
    cloud_need_aws = also_cloud in {"S3", "MINIO"}
    cloud_need_azure = also_cloud == "AZURE"
    cloud_need_gcp = also_cloud == "GCP"

    # Redis: enable if non-LOCAL, or explicitly requested for LOCAL,
    # or when a cloud companion is requested (common in hybrid workflows).
    need_redis = (storage != "LOCAL") or args.redis_with_local or bool(also_cloud)

    # Assemble env
    kv: Dict[str, str] = {"STORAGE_TYPE": storage}

    # Workspace/home
    if args.home:
        kv["SUPERTABLE_HOME"] = args.home

    # LOCAL
    if storage == "LOCAL":
        kv["SUPERTABLE_HOME"] = args.home or args.local_home
        if args.create_local_home:
            try:
                Path(kv["SUPERTABLE_HOME"]).mkdir(parents=True, exist_ok=True)
                print(
                    f"[OK] Ensured local directory: {Path(kv['SUPERTABLE_HOME']).resolve()}"
                )
            except Exception as e:
                print(
                    f"[WARN] Could not create local directory "
                    f"'{kv['SUPERTABLE_HOME']}': {e}",
                    file=sys.stderr,
                )

    # AWS / MinIO (primary)
    if need_aws:
        kv.update(
            {
                "AWS_ACCESS_KEY_ID": args.aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": args.aws_secret_access_key,
                "AWS_DEFAULT_REGION": args.aws_region,
            }
        )
        if args.aws_endpoint_url:
            kv["AWS_S3_ENDPOINT_URL"] = args.aws_endpoint_url
        if str(args.aws_force_path_style).strip() != "":
            kv["AWS_S3_FORCE_PATH_STYLE"] = (
                "true" if _norm_bool(args.aws_force_path_style) else "false"
            )

    # Azure (primary)
    if need_azure:
        _maybe_parse_abfss_for_azure_home(args, kv)
        # Honor explicit flags (override parsed values)
        if args.azure_connection_string:
            kv["AZURE_STORAGE_CONNECTION_STRING"] = args.azure_connection_string
        else:
            if args.azure_account:
                kv["AZURE_STORAGE_ACCOUNT"] = args.azure_account
            if args.azure_key:
                kv["AZURE_STORAGE_KEY"] = args.azure_key
            if args.azure_sas:
                kv["AZURE_SAS_TOKEN"] = args.azure_sas
            if args.azure_endpoint:
                kv["AZURE_BLOB_ENDPOINT"] = args.azure_endpoint
            if args.azure_container:
                kv["AZURE_CONTAINER"] = args.azure_container
            if args.azure_prefix:
                kv["SUPERTABLE_PREFIX"] = args.azure_prefix

        # Hint that AAD is expected when no secret provided
        if not any(
            kv.get(k)
            for k in (
                "AZURE_STORAGE_KEY",
                "AZURE_SAS_TOKEN",
                "AZURE_STORAGE_CONNECTION_STRING",
            )
        ):
            kv["AZURE_AUTH_MODE"] = "AAD"  # informational

    # GCP (primary)
    if need_gcp:
        if args.gcp_project:
            kv["GCP_PROJECT"] = args.gcp_project
        if args.gcp_credentials:
            # If path exists, set GOOGLE_APPLICATION_CREDENTIALS; else keep raw JSON in GCP_SA_JSON
            if Path(args.gcp_credentials).exists():
                kv["GOOGLE_APPLICATION_CREDENTIALS"] = args.gcp_credentials
            else:
                kv["GCP_SA_JSON"] = args.gcp_credentials  # optional pathless mode

    # CLOUD_ companion profile (secondary)
    if also_cloud:
        kv["CLOUD_STORAGE_TYPE"] = also_cloud

        if storage == "LOCAL" and not kv.get("SUPERTABLE_HOME"):
            kv["SUPERTABLE_HOME"] = args.local_home

        if cloud_need_aws:
            kv.update(
                {
                    "CLOUD_AWS_ACCESS_KEY_ID": args.cloud_aws_access_key_id,
                    "CLOUD_AWS_SECRET_ACCESS_KEY": args.cloud_aws_secret_access_key,
                    "CLOUD_AWS_DEFAULT_REGION": args.cloud_aws_region,
                }
            )
            if args.cloud_aws_endpoint_url:
                kv["CLOUD_AWS_S3_ENDPOINT_URL"] = args.cloud_aws_endpoint_url
            if str(args.cloud_aws_force_path_style).strip() != "":
                kv["CLOUD_AWS_S3_FORCE_PATH_STYLE"] = (
                    "true" if _norm_bool(args.cloud_aws_force_path_style) else "false"
                )

        if cloud_need_azure:
            # If primary --home is ABFSS we can also reuse parsing for CLOUD_, but
            # in most cases users point LOCAL home; so only apply when helpful.
            if args.home and args.home.lower().startswith("abfss://"):
                _maybe_parse_abfss_for_azure_home(args, kv, prefix="CLOUD_")

            if args.cloud_azure_connection_string:
                kv["CLOUD_AZURE_STORAGE_CONNECTION_STRING"] = (
                    args.cloud_azure_connection_string
                )
            else:
                if args.cloud_azure_account:
                    kv["CLOUD_AZURE_STORAGE_ACCOUNT"] = args.cloud_azure_account
                if args.cloud_azure_key:
                    kv["CLOUD_AZURE_STORAGE_KEY"] = args.cloud_azure_key
                if args.cloud_azure_sas:
                    kv["CLOUD_AZURE_SAS_TOKEN"] = args.cloud_azure_sas
                if args.cloud_azure_endpoint:
                    kv["CLOUD_AZURE_BLOB_ENDPOINT"] = args.cloud_azure_endpoint
                if args.cloud_azure_container:
                    kv["CLOUD_AZURE_CONTAINER"] = args.cloud_azure_container
                if args.cloud_azure_prefix:
                    kv["CLOUD_SUPERTABLE_PREFIX"] = args.cloud_azure_prefix

            if not any(
                kv.get(k)
                for k in (
                    "CLOUD_AZURE_STORAGE_KEY",
                    "CLOUD_AZURE_SAS_TOKEN",
                    "CLOUD_AZURE_STORAGE_CONNECTION_STRING",
                )
            ):
                kv["CLOUD_AZURE_AUTH_MODE"] = "AAD"

        if cloud_need_gcp:
            if args.cloud_gcp_project:
                kv["CLOUD_GCP_PROJECT"] = args.cloud_gcp_project
            if args.cloud_gcp_credentials:
                if Path(args.cloud_gcp_credentials).exists():
                    kv["CLOUD_GOOGLE_APPLICATION_CREDENTIALS"] = (
                        args.cloud_gcp_credentials
                    )
                else:
                    kv["CLOUD_GCP_SA_JSON"] = args.cloud_gcp_credentials

    # Redis (used for all non-LOCAL; optional on LOCAL; auto-enabled when also-cloud)
    if need_redis:
        if args.redis_url:
            kv["REDIS_URL"] = args.redis_url
        else:
            kv.update(
                {
                    "REDIS_HOST": args.redis_host,
                    "REDIS_PORT": str(args.redis_port),
                    "REDIS_DB": str(args.redis_db),
                    "REDIS_PASSWORD": args.redis_password,
                    "REDIS_SSL": "true" if args.redis_ssl else "false",
                }
            )

    # -----------------------
    # Validation
    # -----------------------
    ok_all = True

    if not args.no_validate:
        # Redis
        if need_redis:
            ok_r, msg_r = validate_redis(
                kv.get("REDIS_URL", ""),
                kv.get("REDIS_HOST", "localhost"),
                int(kv.get("REDIS_PORT", "6379")),
                int(kv.get("REDIS_DB", "0")),
                kv.get("REDIS_PASSWORD", "") or None,
                _norm_bool(kv.get("REDIS_SSL", "false")),
            )
            print(f"[INFO] Redis validation: {msg_r}")
            ok_all = ok_all and ok_r

        # S3/MinIO (primary)
        if need_aws:
            ok_a, msg_a = validate_s3_or_minio(
                kv.get("AWS_ACCESS_KEY_ID", ""),
                kv.get("AWS_SECRET_ACCESS_KEY", ""),
                kv.get("AWS_DEFAULT_REGION", ""),
                kv.get("AWS_S3_ENDPOINT_URL", ""),
                _norm_bool(kv.get("AWS_S3_FORCE_PATH_STYLE", "false")),
                storage,
            )
            print(f"[INFO] {storage} validation: {msg_a}")
            ok_all = ok_all and ok_a

        # Azure (primary)
        if need_azure:
            ok_z, msg_z = validate_azure(
                kv.get("AZURE_STORAGE_ACCOUNT", ""),
                kv.get("AZURE_STORAGE_KEY", ""),
                kv.get("AZURE_STORAGE_CONNECTION_STRING", ""),
                kv.get("AZURE_SAS_TOKEN", ""),
                kv.get("AZURE_BLOB_ENDPOINT", ""),
            )
            print(f"[INFO] AZURE validation: {msg_z}")
            ok_all = ok_all and ok_z

        # GCP (primary)
        if need_gcp:
            creds = kv.get(
                "GOOGLE_APPLICATION_CREDENTIALS", kv.get("GCP_SA_JSON", "")
            )
            ok_g, msg_g = validate_gcp(kv.get("GCP_PROJECT", ""), creds)
            print(f"[INFO] GCP validation: {msg_g}")
            ok_all = ok_all and ok_g

        # CLOUD_ (secondary)
        if also_cloud:
            if cloud_need_aws:
                ok_ca, msg_ca = validate_s3_or_minio(
                    kv.get("CLOUD_AWS_ACCESS_KEY_ID", ""),
                    kv.get("CLOUD_AWS_SECRET_ACCESS_KEY", ""),
                    kv.get("CLOUD_AWS_DEFAULT_REGION", ""),
                    kv.get("CLOUD_AWS_S3_ENDPOINT_URL", ""),
                    _norm_bool(kv.get("CLOUD_AWS_S3_FORCE_PATH_STYLE", "false")),
                    also_cloud,
                )
                print(f"[INFO] CLOUD_{also_cloud} validation: {msg_ca}")
                ok_all = ok_all and ok_ca

            if cloud_need_azure:
                ok_cz, msg_cz = validate_azure(
                    kv.get("CLOUD_AZURE_STORAGE_ACCOUNT", ""),
                    kv.get("CLOUD_AZURE_STORAGE_KEY", ""),
                    kv.get("CLOUD_AZURE_STORAGE_CONNECTION_STRING", ""),
                    kv.get("CLOUD_AZURE_SAS_TOKEN", ""),
                    kv.get("CLOUD_AZURE_BLOB_ENDPOINT", ""),
                )
                print(f"[INFO] CLOUD_AZURE validation: {msg_cz}")
                ok_all = ok_all and ok_cz

            if cloud_need_gcp:
                cloud_creds = kv.get(
                    "CLOUD_GOOGLE_APPLICATION_CREDENTIALS", kv.get("CLOUD_GCP_SA_JSON", "")
                )
                ok_cg, msg_cg = validate_gcp(kv.get("CLOUD_GCP_PROJECT", ""), cloud_creds)
                print(f"[INFO] CLOUD_GCP validation: {msg_cg}")
                ok_all = ok_all and ok_cg

    # Summaries
    print(f"[INFO] STORAGE_TYPE={storage}")
    if "SUPERTABLE_HOME" in kv:
        print(f"[INFO] SUPERTABLE_HOME={kv['SUPERTABLE_HOME']}")
    if need_azure:
        print(
            "[INFO] AZURE_ACCOUNT="
            f"{kv.get('AZURE_STORAGE_ACCOUNT','')}, CONTAINER="
            f"{kv.get('AZURE_CONTAINER','')}, PREFIX="
            f"{kv.get('SUPERTABLE_PREFIX','')}"
        )
    if also_cloud:
        print(f"[INFO] CLOUD_STORAGE_TYPE={also_cloud}")
        if cloud_need_azure:
            print(
                "[INFO] CLOUD_AZURE_ACCOUNT="
                f"{kv.get('CLOUD_AZURE_STORAGE_ACCOUNT','')}, CONTAINER="
                f"{kv.get('CLOUD_AZURE_CONTAINER','')}, PREFIX="
                f"{kv.get('CLOUD_SUPERTABLE_PREFIX','')}"
            )

    # -----------------------
    # Output
    # -----------------------
    if args.write == "-":
        for k, v in kv.items():
            print(f"export {k}={v}")
    else:
        dest = Path(args.write)
        write_env_file(dest, kv)
        print(f"[OK] Wrote {dest.resolve()}")

    return 0 if ok_all or args.no_validate else 2


if __name__ == "__main__":
    raise SystemExit(main())
