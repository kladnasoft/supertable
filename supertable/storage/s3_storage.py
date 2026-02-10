import io
import json
import fnmatch
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.storage.storage_interface import StorageInterface


class S3Storage(StorageInterface):
    """
    AWS S3 backend with LocalStorage parity:
    - list_files(): one-level listing under a prefix, pattern applied to child basename
    - delete(): deletes a single object if it exists, otherwise deletes all objects under prefix
    """

    def __init__(
            self,
            bucket_name: str,
            client=None,
            endpoint_url: Optional[str] = None,
            region: Optional[str] = None,
            url_style: str = "vhost",
            secure: Optional[bool] = None,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            aws_session_token: Optional[str] = None,
            **_: Any,
    ):
        self.bucket_name = bucket_name
        if endpoint_url and "://" not in endpoint_url:
            endpoint_url = "https://" + endpoint_url
        if endpoint_url:
            # Normalize bucket-prefixed AWS endpoints like https://<bucket>.s3.amazonaws.com
            try:
                parsed_ep = urlparse(endpoint_url)
                host_ep = parsed_ep.netloc
                if host_ep.startswith(f"{self.bucket_name}."):
                    endpoint_url = f"{parsed_ep.scheme}://{host_ep[len(self.bucket_name) + 1:]}"
            except Exception:
                pass

        addressing_style = "path" if url_style == "path" else "virtual"
        config = Config(s3={"addressing_style": addressing_style})
        self._config = config

        self._endpoint_url_arg: Optional[str] = endpoint_url
        self._aws_access_key_id: Optional[str] = aws_access_key_id
        self._aws_secret_access_key: Optional[str] = aws_secret_access_key
        self._aws_session_token: Optional[str] = aws_session_token
        self._bucket_region_checked: bool = False

        self.client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            config=config,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

        self.endpoint_url: Optional[str] = self._endpoint_url_arg or getattr(self.client.meta, "endpoint_url", None)
        self.region: Optional[str] = region or getattr(self.client.meta, "region_name", None)
        self.url_style: str = url_style
        if secure is None:
            parsed = urlparse(self.endpoint_url or "")
            self.secure: bool = parsed.scheme != "http"
        else:
            self.secure = secure

    @classmethod
    def from_env(cls) -> "S3Storage":
        bucket = os.getenv("STORAGE_BUCKET") or "supertable"
        region = (
                os.getenv("STORAGE_REGION")
                or os.getenv("AWS_DEFAULT_REGION")
                or os.getenv("AWS_REGION")
                or None
        )
        endpoint = os.getenv("STORAGE_ENDPOINT_URL") or None

        access_key = os.getenv("STORAGE_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or None
        secret_key = os.getenv("STORAGE_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or None
        session_token = os.getenv("AWS_SESSION_TOKEN") or None

        force_path_style = (os.getenv("STORAGE_FORCE_PATH_STYLE", "") or "").lower() in ("1", "true", "yes", "on")
        url_style = "path" if force_path_style else "vhost"

        if endpoint and "://" not in endpoint:
            endpoint = "https://" + endpoint

        if endpoint:
            # Normalize bucket-prefixed AWS endpoints like https://<bucket>.s3.amazonaws.com
            try:
                parsed_ep = urlparse(endpoint)
                host_ep = parsed_ep.netloc
                if host_ep.startswith(f"{bucket}."):
                    endpoint = f"{parsed_ep.scheme}://{host_ep[len(bucket) + 1:]}"
            except Exception:
                pass

        config = Config(s3={"addressing_style": "path" if force_path_style else "virtual"})
        client = boto3.client(
            "s3",
            region_name=region or None,
            endpoint_url=endpoint or None,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            config=config,
        )

        parsed = urlparse(endpoint or getattr(client.meta, "endpoint_url", "") or "")
        secure = parsed.scheme != "http"

        return cls(
            bucket_name=bucket,
            client=client,
            endpoint_url=endpoint or getattr(client.meta, "endpoint_url", None),
            region=region or getattr(client.meta, "region_name", None),
            url_style=url_style,
            secure=secure,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
        )

    def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str:
        prefer_httpfs = (
            prefer_httpfs if prefer_httpfs is not None
            else (os.getenv("SUPERTABLE_DUCKDB_USE_HTTPFS", "") or "").lower() in ("1", "true", "yes", "on")
        )
        key = (key or "").lstrip("/")
        if prefer_httpfs:
            parsed = urlparse(self.endpoint_url or "")
            host = parsed.netloc or parsed.path or ""
            scheme = parsed.scheme or ("https" if self.secure else "http")
            if self.url_style == "vhost":
                return f"{scheme}://{self.bucket_name}.{host}/{key}"
            return f"{scheme}://{host}/{self.bucket_name}/{key}"
        return f"s3://{self.bucket_name}/{key}"

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        self._ensure_bucket_region()
        return self.client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket_name, "Key": key.lstrip("/")},
            ExpiresIn=expiry_seconds,
        )

    # -------------------------
    # Helpers
    # -------------------------

    def _extract_expected_region_from_error(self, e: ClientError) -> Optional[str]:
        # Prefer explicit fields / headers when present.
        err = e.response.get("Error", {}) or {}
        region = err.get("Region")
        if region:
            return self._normalize_bucket_region(str(region))

        headers = (e.response.get("ResponseMetadata", {}) or {}).get("HTTPHeaders", {}) or {}
        hdr_region = headers.get("x-amz-bucket-region") or headers.get("x-amz-region")
        if hdr_region:
            return self._normalize_bucket_region(str(hdr_region))

        # Fallback: try parsing message like "... expecting 'eu-central-1'".
        msg = err.get("Message") or ""
        m = re.search(r"expecting ['\"]([a-z0-9-]+)['\"]", str(msg), re.IGNORECASE)
        return self._normalize_bucket_region(m.group(1) if m else None)

    def _extract_expected_endpoint_url_from_error(self, e: ClientError) -> Optional[str]:
        err = e.response.get("Error", {}) or {}
        endpoint = err.get("Endpoint")
        if not endpoint:
            return None

        endpoint = str(endpoint).strip()
        if "://" in endpoint:
            parsed = urlparse(endpoint)
            scheme = parsed.scheme
            host = parsed.netloc
        else:
            scheme = "https" if self.secure else "http"
            host = endpoint

        host = host.strip().rstrip("/")
        if host.startswith(f"{self.bucket_name}."):
            host = host[len(self.bucket_name) + 1:]

        if not host:
            return None
        return f"{scheme}://{host}"

    def _normalize_bucket_region(self, region: Optional[str]) -> Optional[str]:
        if not region:
            return None
        r = str(region)
        # Legacy AWS responses
        if r == "EU":
            return "eu-west-1"
        if r == "US":
            return "us-east-1"
        return r

    def _aws_endpoint_region(self, endpoint_url: Optional[str]) -> Optional[str]:
        if not endpoint_url:
            return None
        try:
            parsed = urlparse(endpoint_url)
            host = (parsed.netloc or parsed.path or "").lower()
        except Exception:
            return None

        # Strip any bucket prefix from virtual-hosted style endpoint urls.
        if host.startswith(f"{self.bucket_name}."):
            host = host[len(self.bucket_name) + 1:]

        if host in ("s3.amazonaws.com", "s3-external-1.amazonaws.com"):
            return "us-east-1"

        if "amazonaws.com" not in host:
            return None

        m = re.search(r"(^|\.)s3[.-]([a-z0-9-]+)\.", host)
        if not m:
            return None
        return m.group(2)

    def _extract_expected_endpoint_url_from_location_header(self, e: ClientError) -> Optional[str]:
        headers = (e.response.get("ResponseMetadata", {}) or {}).get("HTTPHeaders", {}) or {}
        location = headers.get("location") or headers.get("Location")
        if not location:
            return None
        try:
            parsed = urlparse(str(location))
        except Exception:
            return None
        scheme = parsed.scheme or ("https" if self.secure else "http")
        host = parsed.netloc or ""
        host = host.strip().rstrip("/")
        if host.startswith(f"{self.bucket_name}."):
            host = host[len(self.bucket_name) + 1:]

        if not host:
            return None
        return f"{scheme}://{host}"

    def _is_aws_global_endpoint(self, endpoint_url: Optional[str]) -> bool:
        if not endpoint_url:
            return False
        try:
            parsed = urlparse(endpoint_url)
            host = (parsed.netloc or "").lower()
        except Exception:
            return False
        # The most common "wrong region" case is forcing the global endpoint.
        if host.startswith(f"{self.bucket_name}."):
            host = host[len(self.bucket_name) + 1:]
        return host in ("s3.amazonaws.com", "s3-external-1.amazonaws.com")

    def _probe_bucket_region(self) -> Optional[str]:
        """Best-effort detection of the bucket region without relying on the current client.

        This intentionally bypasses self._call() to avoid recursion when the current client is
        misconfigured for the bucket's region/endpoint.
        """
        try:
            probe = boto3.client(
                "s3",
                region_name="us-east-1",
                endpoint_url=None,
                config=self._config,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                aws_session_token=self._aws_session_token,
            )
        except Exception:
            return None

        # Try GetBucketLocation first (cleanest), then fall back to HeadBucket headers.
        try:
            resp = probe.get_bucket_location(Bucket=self.bucket_name)
            loc = resp.get("LocationConstraint")
            if not loc:
                return "us-east-1"
            return self._normalize_bucket_region(str(loc))
        except ClientError:
            pass

        try:
            probe.head_bucket(Bucket=self.bucket_name)
            # If head_bucket succeeds, region is probe region (unlikely unless us-east-1).
            return "us-east-1"
        except ClientError as e:
            headers = (e.response.get("ResponseMetadata", {}) or {}).get("HTTPHeaders", {}) or {}
            hdr_region = headers.get("x-amz-bucket-region") or headers.get("x-amz-region")
            return self._normalize_bucket_region(str(hdr_region) if hdr_region else None)
        except Exception:
            return None

    def _rebuild_client(self) -> None:
        self.client = boto3.client(
            "s3",
            endpoint_url=self._endpoint_url_arg,
            region_name=self.region,
            config=self._config,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
        )
        self.endpoint_url = self._endpoint_url_arg or getattr(self.client.meta, "endpoint_url", None)
        self.region = self.region or getattr(self.client.meta, "region_name", None)

        parsed = urlparse(self.endpoint_url or "")
        if parsed.scheme:
            self.secure = parsed.scheme != "http"

    def _call(self, method: str, **kwargs: Any) -> Any:
        attempts = 0
        while True:
            try:
                return getattr(self.client, method)(**kwargs)
            except ClientError as e:
                if attempts >= 1:
                    raise

                error_resp = e.response.get("Error", {}) or {}
                code = error_resp.get("Code")

                # Check for redirect codes.
                if code not in (
                        "PermanentRedirect",
                        "301",
                        "Redirect",
                        "AuthorizationHeaderMalformed",
                        "IllegalLocationConstraintException",
                ):
                    raise

                changed = False

                region = self._extract_expected_region_from_error(e) or self._probe_bucket_region()
                if region and region != self.region:
                    self.region = region
                    changed = True

                endpoint_url = (
                        self._extract_expected_endpoint_url_from_error(e)
                        or self._extract_expected_endpoint_url_from_location_header(e)
                )
                if endpoint_url and endpoint_url != self._endpoint_url_arg:
                    self._endpoint_url_arg = endpoint_url
                    changed = True

                # If we can infer region from a redirect location, apply it.
                if not region and endpoint_url:
                    inferred_region = self._aws_endpoint_region(endpoint_url)
                    if inferred_region and inferred_region != self.region:
                        self.region = inferred_region
                        region = inferred_region
                        changed = True

                # If an AWS endpoint was forced for a different region, drop/adjust it.
                current_ep_region = self._aws_endpoint_region(self._endpoint_url_arg)
                if region and current_ep_region and current_ep_region != region:
                    # Prefer the explicit redirect target if present, otherwise let botocore
                    # select the regional endpoint based on the corrected region.
                    self._endpoint_url_arg = endpoint_url or None
                    changed = True

                # If a global AWS endpoint was forced (common in env setups), drop it once we
                # know the bucket is not in us-east-1 so botocore can select the correct
                # regional endpoint.
                if (
                        not endpoint_url
                        and region
                        and region != "us-east-1"
                        and self._is_aws_global_endpoint(self._endpoint_url_arg)
                ):
                    self._endpoint_url_arg = None
                    changed = True

                if not changed:
                    raise

                self._rebuild_client()
                attempts += 1

    def _ensure_bucket_region(self) -> None:
        if self._bucket_region_checked:
            return
        self._bucket_region_checked = True
        try:
            self._call("head_bucket", Bucket=self.bucket_name)
        except ClientError:
            # Ignore permission / existence errors: we only want redirect hints.
            pass

    def _object_exists(self, key: str) -> bool:
        try:
            self._call("head_object", Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def _list_common_prefixes_and_objects_one_level(self, prefix: str) -> List[str]:
        """
        Return immediate child names under prefix using Delimiter="/".
        """
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        self._ensure_bucket_region()

        paginator = self.client.get_paginator("list_objects_v2")
        page_it = paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter="/"
        )

        children = []
        seen = set()

        for page in page_it:
            for cp in page.get("CommonPrefixes", []):
                part = cp["Prefix"][len(prefix):].rstrip("/")
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key == prefix:
                    continue  # folder marker
                part = key[len(prefix):]
                if "/" in part:
                    # deeper level; Delimiter should prevent this, but guard anyway
                    part = part.split("/", 1)[0]
                if part and part not in seen:
                    seen.add(part)
                    children.append(part)

        return children

    # -------------------------
    # JSON
    # -------------------------
    def read_json(self, path: str) -> Dict[str, Any]:
        self._ensure_bucket_region()
        try:
            resp = self._call("get_object", Bucket=self.bucket_name, Key=path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise
        data = resp["Body"].read()
        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")
        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

        # unreachable

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        self._ensure_bucket_region()
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self._call("put_object",
                   Bucket=self.bucket_name,
                   Key=path,
                   Body=payload,
                   ContentType="application/json",
                   )

    # -------------------------
    # Existence / size / makedirs
    # -------------------------
    def exists(self, path: str) -> bool:
        return self._object_exists(path)

    def size(self, path: str) -> int:
        try:
            resp = self._call("head_object", Bucket=self.bucket_name, Key=path)
            return int(resp["ContentLength"])
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def makedirs(self, path: str) -> None:
        # No-op for object storage; see MinIO note if you want folder markers.
        pass

    # -------------------------
    # Listing
    # -------------------------
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Local parity: one-level children under prefix `path`, fnmatch on child name.
        """
        if path and not path.endswith("/"):
            path = path + "/"

        self._ensure_bucket_region()

        children = self._list_common_prefixes_and_objects_one_level(path)
        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        return [path + c for c in filtered]

    # -------------------------
    # Delete
    # -------------------------
    def delete(self, path: str) -> None:
        # exact object?
        if self._object_exists(path):
            self._call("delete_object", Bucket=self.bucket_name, Key=path)
            return

        # prefix recursive
        prefix = path if path.endswith("/") else f"{path}/"

        self._ensure_bucket_region()
        paginator = self.client.get_paginator("list_objects_v2")
        page_it = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        keys = []
        for page in page_it:
            for obj in page.get("Contents", []):
                keys.append({"Key": obj["Key"]})

        if not keys:
            raise FileNotFoundError(f"File or folder not found: {path}")

        # Batch delete in chunks of 1000 (S3 limit)
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]
            self._call("delete_objects",
                       Bucket=self.bucket_name,
                       Delete={"Objects": chunk, "Quiet": True},
                       )

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        root = {}
        if path and not path.endswith("/"):
            path = path + "/"

        self._ensure_bucket_region()

        paginator = self.client.get_paginator("list_objects_v2")
        page_it = paginator.paginate(Bucket=self.bucket_name, Prefix=path)

        for page in page_it:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                suffix = key[len(path):] if path else key
                parts = [p for p in suffix.split("/") if p]
                if not parts:
                    continue
                cursor = root
                for i, part in enumerate(parts):
                    if i == len(parts) - 1:
                        cursor[part] = None
                    else:
                        cursor = cursor.setdefault(part, {})
        return root

    # -------------------------
    # Parquet
    # -------------------------
    def write_parquet(self, table: pa.Table, path: str) -> None:
        self._ensure_bucket_region()
        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()
        self._call("put_object",
                   Bucket=self.bucket_name,
                   Key=path,
                   Body=data,
                   ContentType="application/octet-stream",
                   )

    def read_parquet(self, path: str) -> pa.Table:
        self._ensure_bucket_region()
        try:
            resp = self._call("get_object", Bucket=self.bucket_name, Key=path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"Parquet file not found: {path}") from e
            raise
        data = resp["Body"].read()
        try:
            return pq.read_table(io.BytesIO(data))
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet at '{path}': {e}")

    # -------------------------
    # Bytes / Text / Copy
    # -------------------------
    def write_bytes(self, path: str, data: bytes) -> None:
        self._ensure_bucket_region()
        self._call("put_object",
                   Bucket=self.bucket_name, Key=path, Body=data, ContentType="application/octet-stream"
                   )

    def read_bytes(self, path: str) -> bytes:
        self._ensure_bucket_region()
        try:
            resp = self._call("get_object", Bucket=self.bucket_name, Key=path)
            return resp["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def copy(self, src_path: str, dst_path: str) -> None:
        self._ensure_bucket_region()
        self._call("copy_object",
                   Bucket=self.bucket_name,
                   Key=dst_path,
                   CopySource={"Bucket": self.bucket_name, "Key": src_path},
                   )