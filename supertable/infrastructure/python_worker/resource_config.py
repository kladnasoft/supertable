from dataclasses import dataclass

@dataclass
class ResourceConfig:
    mem_limit: str = "128m"
    cpu_period: int = 100000
    cpu_quota: int = 50000  # 50% of one CPU
    network_disabled: bool = True
    timeout: int = 10
    max_output_len: int = 100_000  # 100KB limit for safety

# Pre-defined profiles
LOW_TIER = ResourceConfig(mem_limit="128m", cpu_quota=20000) # 20% CPU
HIGH_TIER = ResourceConfig(mem_limit="1gb", cpu_quota=200000, network_disabled=False) # 2 CPUs + Internet