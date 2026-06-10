"""
Shared defaults for the synthetic webshop demo (data generation + loading).

Used by:
  - generate.py — one-shot historical generation
  - load.py     — load generated parquet files into SuperTable
  - topup.py    — continuous top-up against SuperTable
  - core.py     — generator engine (referenced indirectly via the above)
"""

# --- SuperTable connection settings ---
organization: str = "kladna-soft"
super_name: str = "webshop"
role_name: str = "superadmin"

# --- Data generation / loading settings ---
generated_data_dir: str = "webshop_demo_data"

# Columns used for upsert deduplication during loading.
# Empty list means no overwrite-based dedup (append mode).
overwrite_columns: list[str] = []
