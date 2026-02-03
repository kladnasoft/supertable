# execute_v20260201_2045_fix-script.py
from supertable.notebook.worker import ExecutionWorker
from supertable.notebook.resource_config import ResourceConfig
from supertable.notebook.warm_pool_manager import WarmPoolManager

worker = ExecutionWorker()

print("--- Testing Cold Worker ---")
free_config = ResourceConfig(mem_limit="64m", timeout=5)
print(f"Low Tier Result: {worker.run_code('print(2 + 2)', config=free_config)}")

pro_config = ResourceConfig(mem_limit="2gb", timeout=60, network_disabled=False)
# Added timeout to requests to avoid hanging
code_high = "import requests; r = requests.get('https://api.github.com', timeout=5); print(f'GitHub: {r.status_code}')"
print(f"High Tier Result: {worker.run_code(code_high, config=pro_config)}")

print("\n--- Testing Warm Pool ---")
notebook_pool = WarmPoolManager(pool_size=3)

def on_cell_run(code):
    return notebook_pool.execute_sync(code)

print(f"Warm Pool Result: {on_cell_run('print(10 + 10)')}")