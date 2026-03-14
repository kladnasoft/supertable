"""
Shared defaults for synthetic webshop data generation and loading.

Used by:
  - synthetic_webshop_data_generator.py
  - synthetic_webshop_data_loader.py
  - synthetic_webshop_data_core.py
"""

# --- SuperTable connection settings ---
organization: str = "kladna-soft"
super_name: str = "example"
role_name: str = "superadmin"

# --- Data generation / loading settings ---
generated_data_dir: str = "webshop_demo_data"

# Columns used for upsert deduplication during loading.
# Empty list means no overwrite-based dedup (append mode).
overwrite_columns: list[str] = []
