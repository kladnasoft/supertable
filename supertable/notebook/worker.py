# worker_v20260201_2045_add-dns.py
import docker
from supertable.notebook.resource_config import ResourceConfig


class ExecutionWorker:
    def __init__(self):
        self.client = docker.from_env(timeout=30)
        self.image_name = "code-runner-slim"

    def run_code(self, code_str: str, config: ResourceConfig):
        try:
            # Explicit DNS to fix resolution errors in networked containers
            dns_servers = ["8.8.8.8", "8.8.4.4"] if not config.network_disabled else None

            container = self.client.containers.run(
                image=self.image_name,
                command=["python3", "-c", code_str],
                detach=True,
                mem_limit=config.mem_limit,
                cpu_period=config.cpu_period,
                cpu_quota=config.cpu_quota,
                network_disabled=config.network_disabled,
                dns=dns_servers,
                security_opt=["no-new-privileges"]
            )
            try:
                status = container.wait(timeout=config.timeout)
                logs = container.logs().decode('utf-8', errors='replace')
                return {"status": "success", "exit_code": status["StatusCode"], "output": logs}
            except Exception:
                try:
                    container.kill()
                except:
                    pass
                return {"status": "timeout", "error": f"Execution exceeded {config.timeout}s"}
            finally:
                try:
                    container.remove()
                except:
                    pass
        except Exception as e:
            return {"status": "system_error", "error": str(e)}