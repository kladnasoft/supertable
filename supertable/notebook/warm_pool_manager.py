# warm_pool_manager_v20260201_2055_bg-cleanup.py
import docker
import queue
import threading
import time
from supertable.notebook.resource_config import ResourceConfig
from typing import Set


class WarmPoolManager:
    # Global active container tracking to safely support multiple pools
    _GLOBAL_ACTIVE_CONTAINER_IDS: Set[str] = set()
    _GLOBAL_LOCK = threading.Lock()

    def __init__(self, pool_size=5, config=ResourceConfig()):
        self.client = docker.from_env(timeout=60)
        self.image_name = "code-runner-slim"
        self.config = config
        self.pool_size = pool_size
        self.pool = queue.Queue()

        # Track all containers currently managed by this instance
        self.active_containers = set()
        self._lock = threading.Lock()

        # Initial cleanup of existing ghosts
        self.cleanup_orphans()

        # Start the background maintenance thread
        self.stop_event = threading.Event()
        self.maintenance_thread = threading.Thread(target=self._bg_maintenance, daemon=True)
        self.maintenance_thread.start()

        for _ in range(self.pool_size):
            try:
                self._add_container_to_pool()
            except Exception as e:
                print(f"Warm pool initialization failed: {e}")
                break

    def cleanup_orphans(self):
        """Force-removes any containers from the code-runner image not in our active set."""
        try:
            filters = {"ancestor": self.image_name}
            all_containers = self.client.containers.list(all=True, filters=filters)

            with self._GLOBAL_LOCK:
                active_ids = set(self._GLOBAL_ACTIVE_CONTAINER_IDS)

            for container in all_containers:
                if container.id not in active_ids:
                    try:
                        container.remove(force=True)
                    except:
                        pass
        except Exception as e:
            print(f"Cleanup error: {e}")

    def _bg_maintenance(self):
        """Periodically scans for leaked containers every minute."""
        while not self.stop_event.is_set():
            # Sleep first to give the system time to settle
            time.sleep(60)
            self.cleanup_orphans()

    def _add_container_to_pool(self):
        dns_servers = ["8.8.8.8", "8.8.4.4"] if not self.config.network_disabled else None
        container = self.client.containers.run(
            image=self.image_name,
            command="tail -f /dev/null",
            detach=True,
            mem_limit=self.config.mem_limit,
            cpu_period=self.config.cpu_period,
            cpu_quota=self.config.cpu_quota,
            network_disabled=self.config.network_disabled,
            dns=dns_servers
        )
        with self._lock:
            self.active_containers.add(container)
        with self._GLOBAL_LOCK:
            self._GLOBAL_ACTIVE_CONTAINER_IDS.add(container.id)
        self.pool.put(container)

    def get_container(self):
        container = self.pool.get()
        threading.Thread(target=self._safe_add_to_pool, daemon=True).start()
        return container

    def _safe_add_to_pool(self):
        try:
            self._add_container_to_pool()
        except Exception as e:
            print(f"Failed to refill warm pool: {e}")

    def execute_sync(self, code_str):
        """Synchronous wrapper for test scripts."""
        output_chunks = []
        for chunk in self.execute_and_stream(code_str):
            output_chunks.append(chunk)
        return "".join(output_chunks).strip()

    def execute_and_stream(self, code_str):
        """Generates output for real-time streaming with size and time caps."""
        container = self.get_container()
        start_time = time.monotonic()
        total_bytes = 0

        try:
            exec_instance = container.exec_run(
                cmd=["python3", "-u", "-c", code_str],
                workdir="/home/sandbox",
                stream=True
            )

            for line in exec_instance.output:
                if time.monotonic() - start_time > self.config.timeout:
                    yield "\n[Error: Timeout]"
                    break

                total_bytes += len(line)
                if total_bytes > self.config.max_output_len:
                    yield f"\n[Error: Output limit of {self.config.max_output_len} bytes exceeded]"
                    break

                yield line.decode('utf-8', errors='replace')

        except Exception as e:
            yield f"Error: {str(e)}"
        finally:
            self._finalize_container(container)

    def _finalize_container(self, container):
        """Ensures container is stopped, removed, and untracked."""
        try:
            container.kill()
        except:
            pass

        try:
            container.remove()
        except:
            pass

        with self._lock:
            if container in self.active_containers:
                self.active_containers.remove(container)

        with self._GLOBAL_LOCK:
            self._GLOBAL_ACTIVE_CONTAINER_IDS.discard(container.id)

    def shutdown_pool(self):
        """Stops maintenance and clears all containers."""
        self.stop_event.set()
        with self._lock:
            # Copy set to avoid mutation errors during iteration
            for container in list(self.active_containers):
                try:
                    container.remove(force=True)
                    with self._GLOBAL_LOCK:
                        self._GLOBAL_ACTIVE_CONTAINER_IDS.discard(container.id)
                except:
                    pass
            self.active_containers.clear()