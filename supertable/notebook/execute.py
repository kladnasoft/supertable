from supertable.notebook.worker import ExecutionWorker
from supertable.notebook.resource_config import ResourceConfig
from supertable.notebook.warm_pool_manager import WarmPoolManager

# 1. Test Cold Worker
worker = ExecutionWorker()

print("--- Testing Cold Worker ---")
free_config = ResourceConfig(mem_limit="64m", timeout=5)
print(f"Low Tier Result: {worker.run_code('print(2 + 2)', config=free_config)}")

pro_config = ResourceConfig(mem_limit="2gb", timeout=60, network_disabled=False)
# Simplified quoting to avoid SyntaxError
code_high = "import requests; r = requests.get('https://api.github.com'); print(f'GitHub: {r.status_code}')"
print(f"High Tier Result: {worker.run_code(code_high, config=pro_config)}")


# 2. Test Warm Pool
print("\n--- Testing Warm Pool ---")
notebook_pool = WarmPoolManager(pool_size=3)

def on_cell_run(code):
    return notebook_pool.execute_in_warm_container(code)

print(f"Warm Pool Result: {on_cell_run('print(10 + 10)')}")