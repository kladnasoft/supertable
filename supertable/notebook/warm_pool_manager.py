import docker
import queue
import threading
from supertable.notebook.resource_config import ResourceConfig


class WarmPoolManager:
    def __init__(self, pool_size=5, config=ResourceConfig()):
        self.client = docker.from_env(timeout=60)
        self.image_name = "code-runner-slim"
        self.config = config
        self.pool_size = pool_size
        self.pool = queue.Queue()

        for _ in range(self.pool_size):
            self._add_container_to_pool()

    def _add_container_to_pool(self):
        container = self.client.containers.run(
            image=self.image_name,
            command="tail -f /dev/null",
            detach=True,
            mem_limit=self.config.mem_limit,
            cpu_quota=self.config.cpu_quota,
            network_disabled=self.config.network_disabled
        )
        self.pool.put(container)

    def get_container(self):
        container = self.pool.get()
        threading.Thread(target=self._add_container_to_pool).start()
        return container

    def execute_and_stream(self, code_str):
        """Generates output line-by-line for real-time streaming."""
        container = self.get_container()
        try:
            # Setting stream=True allows us to iterate over the output
            exec_instance = container.exec_run(
                cmd=["python3", "-u", "-c", code_str],  # -u is for unbuffered output
                workdir="/home/sandbox",
                stream=True
            )

            for line in exec_instance.output:
                yield line.decode('utf-8')

        except Exception as e:
            yield f"Error: {str(e)}"
        finally:
            container.stop(timeout=1)
            container.remove()