# route: supertable.storage.s3_storage
import io
import json
import fnmatch
import os

from supertable.config.settings import settings
import re
from typing import Any, BinaryIO, Dict, List, Optional
from urllib.parse import quote, urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
    StorageInterface,
    normalize_sha256_checksum,
    read_exact_range_body,
    validate_range_request,
    write_all,
)


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
            base_prefix: str = "",
            **_: Any,
    ):
        self.bucket_name = bucket_name
        self.base_prefix = base_prefix.strip("/") if base_prefix else ""
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
        bucket = settings.STORAGE_BUCKET
        region = settings.STORAGE_REGION or None
        endpoint = settings.STORAGE_ENDPOINT_URL or None
        access_key = settings.STORAGE_ACCESS_KEY or None
        secret_key = settings.STORAGE_SECRET_KEY or None
        session_token = settings.STORAGE_SESSION_TOKEN or None
        base_prefix = settings.SUPERTABLE_PREFIX

        force_path_style = settings.STORAGE_FORCE_PATH_STYLE
        url_style = "path" if force_path_style else "vhost"

        # Let __init__ handle endpoint normalisation, client creation, and secure detection
        return cls(
            bucket_name=bucket,
            endpoint_url=endpoint,
            region=region,
            url_style=url_style,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            base_prefix=base_prefix,
        )

    def to_duckdb_path(self, key: str, prefer_httpfs: Optional[bool] = None) -> str:
        prefer_httpfs = (
            prefer_httpfs if prefer_httpfs is not None
            else settings.SUPERTABLE_DUCKDB_USE_HTTPFS
        )
        key = (key or "").lstrip("/")
        full_key = f"{self.base_prefix}/{key}" if self.base_prefix else key
        if prefer_httpfs:
            parsed = urlparse(self.endpoint_url or "")
            host = parsed.netloc or parsed.path or ""
            scheme = parsed.scheme or ("https" if self.secure else "http")
            if self.url_style == "vhost":
                return f"{scheme}://{self.bucket_name}.{host}/{full_key}"
            return f"{scheme}://{host}/{self.bucket_name}/{full_key}"
        return f"s3://{self.bucket_name}/{full_key}"

    def presign(self, key: str, expiry_seconds: int = 3600) -> str:
        self._ensure_bucket_region()
        full_key = self._with_base(key.lstrip("/"))
        return self.client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket_name, "Key": full_key},
            ExpiresIn=expiry_seconds,
        )

    def canonical_uri(self, path: str) -> str:
        """Return the canonical S3 URI for one logical object path."""
        full_key = self._with_base(path)
        return f"s3://{self.bucket_name}/{quote(full_key, safe='/=:@+$,;~-._')}"

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

    def _get_object_safe(self, path: str) -> bytes:
        """
        Download an object and guarantee the StreamingBody is closed,
        even if .read() raises mid-stream (network timeout, corrupt data, etc.).
        """
        resp = self._call("get_object", Bucket=self.bucket_name, Key=path)
        body = resp["Body"]
        try:
            return body.read()
        finally:
            body.close()

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

        children: List[str] = []
        seen: set = set()

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
        path = self._with_base(path)
        self._ensure_bucket_region()
        try:
            data = self._get_object_safe(path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise
        if len(data) == 0:
            raise ValueError(f"File is empty: {path}")
        try:
            return json.loads(data)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        path = self._with_base(path)
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
        return self._object_exists(self._with_base(path))

    def size(self, path: str) -> int:
        path = self._with_base(path)
        try:
            resp = self._call("head_object", Bucket=self.bucket_name, Key=path)
            return int(resp["ContentLength"])
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    @staticmethod
    def _metadata_from_response(response: Dict[str, Any]) -> ObjectMetadata:
        modified = response.get("LastModified")
        modified_ns = int(modified.timestamp() * 1_000_000_000) if modified else 0
        etag = str(response.get("ETag") or "").strip('"')
        checksum_type = str(response.get("ChecksumType") or "").upper()
        # Multipart COMPOSITE checksums are not the SHA-256 of the complete
        # object and therefore must not be compared with the downloaded bytes.
        checksum = ""
        if checksum_type == "FULL_OBJECT" or (not checksum_type and "-" not in etag):
            checksum = normalize_sha256_checksum(response.get("ChecksumSHA256"))
        return ObjectMetadata(
            size=int(response.get("ContentLength") or 0),
            version=str(response.get("VersionId") or ""),
            etag=etag,
            last_modified_ns=modified_ns,
            checksum_sha256=checksum,
        )

    def stat_object(self, path: str) -> ObjectMetadata:
        path = self._with_base(path)
        self._ensure_bucket_region()
        try:
            response = self._call("head_object", Bucket=self.bucket_name, Key=path)
            return self._metadata_from_response(response)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def download_to_file(
        self,
        path: str,
        file_obj: BinaryIO,
        *,
        expected: ObjectMetadata | None = None,
        chunk_size: int = 8 * 1024 * 1024,
    ) -> int:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        path = self._with_base(path)
        self._ensure_bucket_region()
        request: Dict[str, Any] = {"Bucket": self.bucket_name, "Key": path}
        if expected is not None:
            if expected.version:
                request["VersionId"] = expected.version
            if expected.etag:
                etag = expected.etag.strip('"')
                request["IfMatch"] = f'"{etag}"'
        try:
            response = self._call("get_object", **request)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound", "NoSuchVersion"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

        body = response["Body"]
        written = 0
        try:
            if expected is not None:
                response_meta = self._metadata_from_response(response)
                if response_meta.size != expected.size:
                    raise OSError(f"Object changed before download: {path}")
                if response_meta.version and expected.version and response_meta.version != expected.version:
                    raise OSError(f"Object version changed before download: {path}")
                if response_meta.etag and expected.etag and response_meta.etag != expected.etag:
                    raise OSError(f"Object ETag changed before download: {path}")
            while True:
                chunk = body.read(chunk_size)
                if not chunk:
                    break
                written += write_all(file_obj, chunk)
        finally:
            body.close()
        expected_size = expected.size if expected is not None else int(response.get("ContentLength") or written)
        if written != expected_size:
            raise OSError(
                f"Short download for {path}: expected {expected_size} bytes, wrote {written}"
            )
        return written

    def read_range(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        expected: ObjectMetadata | None = None,
    ) -> bytes:
        offset, length = validate_range_request(offset, length, expected)
        if length == 0:
            return b""
        if expected is None or not (expected.version or expected.etag):
            raise ValueError("S3 range reads require a version or ETag condition")
        path = self._with_base(path)
        self._ensure_bucket_region()
        request: Dict[str, Any] = {
            "Bucket": self.bucket_name,
            "Key": path,
            "Range": f"bytes={offset}-{offset + length - 1}",
        }
        if expected is not None:
            if expected.version:
                request["VersionId"] = expected.version
            if expected.etag:
                request["IfMatch"] = f'"{expected.etag.strip(chr(34))}"'
        try:
            response = self._call("get_object", **request)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code") or "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("PreconditionFailed", "NoSuchVersion") or status == 412:
                raise ObjectIdentityMismatch(f"Object version changed: {path}") from exc
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"File not found: {path}") from exc
            raise
        body = response["Body"]
        try:
            payload = read_exact_range_body(body, length)
        finally:
            body.close()
        if expected is not None:
            version = str(response.get("VersionId") or "")
            etag = str(response.get("ETag") or "").strip('"')
            if expected.version and version and version != expected.version:
                raise ObjectIdentityMismatch(f"Object version changed: {path}")
            if expected.etag and etag and etag != expected.etag.strip('"'):
                raise ObjectIdentityMismatch(f"Object ETag changed: {path}")
        return payload

    def cache_namespace(self) -> Dict[str, str]:
        parsed = urlparse(self.endpoint_url or "")
        host = parsed.hostname or ""
        if parsed.port:
            host = f"{host}:{parsed.port}"
        namespace = {"provider": "s3", "bucket": self.bucket_name}
        if host:
            namespace["endpoint_host"] = host.lower()
        if self.region:
            namespace["region"] = str(self.region)
        if self.base_prefix:
            namespace["base_prefix"] = self.base_prefix
        return namespace

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
        path = self._with_base(path)
        if path and not path.endswith("/"):
            path = path + "/"

        self._ensure_bucket_region()

        children = self._list_common_prefixes_and_objects_one_level(path)
        filtered = [c for c in children if fnmatch.fnmatch(c, pattern)]
        filtered.sort()
        return [self._without_base(path + c) for c in filtered]

    # -------------------------
    # Delete
    # -------------------------
    def delete(self, path: str) -> None:
        path = self._with_base(path)
        # exact object?
        if self._object_exists(path):
            self._call("delete_object", Bucket=self.bucket_name, Key=path)
            return

        logical = self._without_base(path)
        if not self._prefix_has_objects(path):
            raise FileNotFoundError(f"File or folder not found: {path}")
        # Compatibility single-pass deletion. Destructive table APIs call the
        # explicit verified ``delete_prefix`` operation below.
        errors: List[Dict[str, Any]] = []
        batch: List[Dict[str, str]] = []
        for key in self._iter_prefix_keys(path):
            batch.append({"Key": key})
            if len(batch) == 1000:
                response = self._call(
                    "delete_objects", Bucket=self.bucket_name,
                    Delete={"Objects": batch, "Quiet": True},
                ) or {}
                errors.extend(response.get("Errors", []) or [])
                batch = []
        if batch:
            response = self._call(
                "delete_objects", Bucket=self.bucket_name,
                Delete={"Objects": batch, "Quiet": True},
            ) or {}
            errors.extend(response.get("Errors", []) or [])
        if errors:
            first = errors[0]
            raise OSError(
                f"S3 partial deletion failure for {logical!r}: "
                f"{first.get('Code')}: {first.get('Message')}"
            )

    def _iter_prefix_keys(self, physical_prefix: str):
        prefix = physical_prefix if physical_prefix.endswith("/") else f"{physical_prefix}/"
        self._ensure_bucket_region()
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key:
                    yield str(key)

    def _prefix_has_objects(self, physical_prefix: str) -> bool:
        return next(self._iter_prefix_keys(physical_prefix), None) is not None

    def delete_prefix(self, path: str) -> None:
        """Delete and verify an S3 prefix, retrying partial batch failures."""
        physical = self._with_base(path)
        if self._object_exists(physical):
            self._call("delete_object", Bucket=self.bucket_name, Key=physical)

        last_errors: List[Dict[str, Any]] = []
        for _attempt in range(3):
            found = False
            batch: List[Dict[str, str]] = []
            last_errors = []
            for key in self._iter_prefix_keys(physical):
                found = True
                batch.append({"Key": key})
                if len(batch) == 1000:
                    response = self._call(
                        "delete_objects", Bucket=self.bucket_name,
                        Delete={"Objects": batch, "Quiet": True},
                    ) or {}
                    last_errors.extend(response.get("Errors", []) or [])
                    batch = []
            if batch:
                response = self._call(
                    "delete_objects", Bucket=self.bucket_name,
                    Delete={"Objects": batch, "Quiet": True},
                ) or {}
                last_errors.extend(response.get("Errors", []) or [])
            empty_after = not found or not self._prefix_has_objects(physical)
            if empty_after and not last_errors:
                return
            if empty_after and last_errors:
                first = last_errors[0]
                raise OSError(
                    f"S3 reported a partial prefix deletion failure for {path!r}: "
                    f"{first.get('Code')}: {first.get('Message')}"
                )

        remaining = next(self._iter_prefix_keys(physical), None)
        detail = ""
        if last_errors:
            first = last_errors[0]
            detail = f"; provider error={first.get('Code')}: {first.get('Message')}"
        raise OSError(
            f"S3 prefix is not empty after verified deletion: {path!r}; "
            f"remaining={remaining!r}{detail}"
        )

    # -------------------------
    # Directory structure
    # -------------------------
    def get_directory_structure(self, path: str) -> dict:
        path = self._with_base(path)
        root: Dict[str, Any] = {}
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
        path = self._with_base(path)
        self._ensure_bucket_region()
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        self._call("put_object",
                   Bucket=self.bucket_name,
                   Key=path,
                   Body=buf,
                   ContentType="application/octet-stream",
                   )

    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> pa.Table:
        path = self._with_base(path)
        self._ensure_bucket_region()
        try:
            data = self._get_object_safe(path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"Parquet file not found: {path}") from e
            raise
        try:
            buf = io.BytesIO(data)
            proj = None
            if columns:
                proj = self._project_columns(pq.read_schema(buf).names, columns)
                buf.seek(0)
            return pq.read_table(buf, columns=proj) if proj else pq.read_table(buf)
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet at '{path}': {e}")

    # -------------------------
    # Bytes / Text / Copy
    # -------------------------
    def write_bytes(self, path: str, data: bytes) -> None:
        path = self._with_base(path)
        self._ensure_bucket_region()
        self._call("put_object",
                   Bucket=self.bucket_name, Key=path, Body=data, ContentType="application/octet-stream"
                   )

    def read_bytes(self, path: str) -> bytes:
        path = self._with_base(path)
        self._ensure_bucket_region()
        try:
            return self._get_object_safe(path)
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
        src_path = self._with_base(src_path)
        dst_path = self._with_base(dst_path)
        self._ensure_bucket_region()
        self._call("copy_object",
                   Bucket=self.bucket_name,
                   Key=dst_path,
                   CopySource={"Bucket": self.bucket_name, "Key": src_path},
                   )
