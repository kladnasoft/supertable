import docker
from supertable.notebook.resource_config import ResourceConfig

class ExecutionWorker:
    def __init__(self):
        # Increased client timeout for stability
        self.client = docker.from_env(timeout=30)
        self.image_name = "code-runner-slim"

    def run_code(self, code_str: str, config: ResourceConfig):
        try:
            container = self.client.containers.run(
                image=self.image_name,
                # Using single quotes for the -c argument helps avoid shell expansion issues
                command=["python3", "-c", code_str],
                detach=True,
                mem_limit=config.mem_limit,
                cpu_period=config.cpu_period,
                cpu_quota=config.cpu_quota,
                network_disabled=config.network_disabled,
                security_opt=["no-new-privileges"]
            )

            try:
                status = container.wait(timeout=config.timeout)
                logs = container.logs().decode('utf-8')
                return {"status": "success", "exit_code": status["StatusCode"], "output": logs}
            except Exception:
                container.kill()
                return {"status": "timeout", "error": f"Execution exceeded {config.timeout}s"}
            finally:
                container.remove()

        except Exception as e:
            return {"status": "system_error", "error": str(e)}