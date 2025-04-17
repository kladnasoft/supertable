import io
import json
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Any, Dict, List
from botocore.exceptions import ClientError

from supertable.storage.storage_interface import StorageInterface


class S3Storage(StorageInterface):
    """
    An AWS S3-based implementation of StorageInterface using the boto3 client.

    Usage Example:
        import boto3

        s3_client = boto3.client(
            "s3",
            aws_access_key_id="YOUR_ACCESS_KEY",
            aws_secret_access_key="YOUR_SECRET_KEY",
            region_name="us-east-1",
        )
        storage = S3Storage(bucket_name="my-bucket", s3_client=s3_client)
    """

    def __init__(self, bucket_name: str, s3_client: Any):
        self.bucket_name = bucket_name
        self.client = s3_client

    def read_json(self, path: str) -> Dict[str, Any]:
        """
        Reads and returns JSON data from S3 at the given object path.
        Raises FileNotFoundError or ValueError on error.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=path)
            data_bytes = response["Body"].read()
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

        if len(data_bytes) == 0:
            raise ValueError(f"File is empty: {path}")

        try:
            return json.loads(data_bytes)
        except json.JSONDecodeError as je:
            raise ValueError(f"Invalid JSON in {path}") from je

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """
        Writes JSON data to the given object path in S3.
        Overwrites if it already exists.
        """
        json_bytes = json.dumps(data).encode("utf-8")
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=path,
                Body=json_bytes,
                ContentType="application/json",
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to write JSON to {path}: {e}") from e

    def exists(self, path: str) -> bool:
        """
        Returns True if the object at 'path' exists in S3; False otherwise.
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=path)
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return False
            raise

    def size(self, path: str) -> int:
        """
        Returns the size in bytes of the object at 'path'.
        Raises FileNotFoundError if the object doesn't exist.
        """
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=path)
            return response["ContentLength"]
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                raise FileNotFoundError(f"File not found: {path}") from e
            raise

    def makedirs(self, path: str) -> None:
        """
        S3 does not have real directories, so this is effectively a no-op.
        If you want to simulate folders, you could create zero-byte objects
        that end with a slash. We'll keep it empty here.
        """
        pass

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        Lists objects in S3 (non-recursive) that start with 'path' as the prefix.
        We'll emulate single-level listing by setting a delimiter and checking
        that there's no further slash beyond the prefix.
        Then we apply 'pattern' filtering with fnmatch.
        """
        results = []
        # Ensure path ends with '/'
        if path and not path.endswith("/"):
            path += "/"

        continuation_token = None
        while True:
            kwargs = {
                "Bucket": self.bucket_name,
                "Prefix": path,
                "Delimiter": "/",  # to simulate non-recursive
            }
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = self.client.list_objects_v2(**kwargs)
            # 'Contents' holds objects at this "directory" level
            contents = response.get("Contents", [])
            for obj in contents:
                key = obj["Key"]
                # If there's an extra slash beyond 'path', it's subfolder content
                sub_part = key[len(path):]
                if "/" not in sub_part:  # no deeper slash => same "directory" level
                    if fnmatch.fnmatch(key, pattern):
                        results.append(key)

            if response.get("IsTruncated"):
                continuation_token = response["NextContinuationToken"]
            else:
                break

        return results

    def delete(self, path: str) -> None:
        """
        Deletes the object at 'path' from S3.
        Raises FileNotFoundError if the object does not exist.
        """
        if not self.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=path)
        except ClientError as e:
            raise RuntimeError(f"Failed to delete {path}: {e}") from e

    def get_directory_structure(self, path: str) -> dict:
        """
        Recursively builds and returns a nested dictionary that represents
        the folder structure under 'path'. For example:
        {
          "subfolder1": {
            "fileA.txt": None,
            "fileB.json": None
          },
          "subfolder2": {
            "nested": {
              "fileC.parquet": None
            }
          }
        }

        We'll do a full recursive list (without a delimiter) of all objects whose keys
        start with `path`, then parse them into a nested dictionary.
        """
        directory_structure = {}
        prefix = path
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        continuation_token = None
        while True:
            kwargs = {
                "Bucket": self.bucket_name,
                "Prefix": prefix,
            }
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = self.client.list_objects_v2(**kwargs)
            for obj in response.get("Contents", []):
                full_key = obj["Key"]
                rel_key = full_key[len(prefix):] if prefix else full_key
                if rel_key.startswith("/"):
                    rel_key = rel_key[1:]
                if not rel_key:
                    continue

                # Split into folders/files
                parts = rel_key.split("/")
                parent = directory_structure
                for subfolder in parts[:-1]:
                    parent = parent.setdefault(subfolder, {})
                # The final part is the filename
                parent[parts[-1]] = None

            if response.get("IsTruncated"):
                continuation_token = response["NextContinuationToken"]
            else:
                break

        return directory_structure

    def write_parquet(self, table: pa.Table, path: str) -> None:
        """
        Writes a PyArrow table to S3 as a Parquet object.
        We'll serialize in-memory, then upload via put_object.
        """
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        data_size = len(buffer.getvalue())
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=path,
                Body=buffer,
                ContentType="application/octet-stream",
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to write Parquet to {path}: {e}") from e
