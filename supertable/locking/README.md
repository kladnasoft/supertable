Locking Package
The Locking package provides a robust and flexible resource locking mechanism for your application. It supports both file-based locking and Redis-based locking, and offers a unified interface that automatically selects the appropriate backend based on your configuration.

Features
File-based Locking:
Uses a JSON file with OS-level file locks to secure resources.

Redis-based Locking:
Leverages Redis’ atomic operations to implement distributed locks. Ideal for cloud or multi-instance environments.

Context Manager Interface:
Supports Python’s with statement for clean and safe resource locking.

Auto-detection of Backend:
Automatically uses Redis if Redis configuration settings (e.g. REDIS_HOST) are present in your supertable.defaults; otherwise, it defaults to file-based locking.

Folder Structure
Organize the package into its own folder for clarity:

locking/
├── __init__.py
├── locking.py
├── file_lock.py
└── redis_lock.py
In your __init__.py, you can expose the main interface:

from .locking import Locking

__all__ = ["Locking"]
Installation
Place the locking folder in your project directory.
Install dependencies:
Python 3.x
redis Python package (install via pip install redis)
Usage
Importing the Locking Interface
Import the main locking class in your code:

from locking import Locking
File-Based Locking Example

with Locking(identity="resource1", backend="file", working_dir="/tmp") as lock:
    # Critical section using file-based locking
    print("File lock acquired.")
Redis-Based Locking Example
If your supertable.defaults is configured with Redis settings, the backend will automatically switch to Redis:

from locking import Locking

# Auto-detection of backend: uses Redis if default.REDIS_HOST is set.
with Locking(identity="resource1") as lock:
    # Critical section using Redis-based locking
    print("Redis lock acquired.")
Custom Redis Configuration
Override defaults by passing parameters directly:

with Locking(identity="resource1", backend="redis", host="redis.mycloud.com", port=6380) as lock:
    print("Custom Redis lock acquired.")
Configuration
The package checks for Redis configuration settings in your supertable.defaults. Ensure the following settings are defined if you plan to use Redis:

REDIS_HOST
REDIS_PORT
REDIS_DB
REDIS_PASSWORD (optional)
DEFAULT_TIMEOUT_SEC
DEFAULT_LOCK_DURATION_SEC
IS_DEBUG (optional)
If these settings are present, the locking backend will be set to Redis automatically unless overridden.

Contributing
Contributions are welcome! If you find any issues or want to add improvements, please open an issue or submit a pull request.

License
This project is licensed under the MIT License.