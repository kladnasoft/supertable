from datetime import date

import supertable.config.homedir
from synthetic_webshop_data_core import GenerationConfig, WebshopDataGenerator
from synthetic_webshop_defaults import generated_data_dir


def main() -> None:
    config = GenerationConfig(
        output_dir=generated_data_dir,
        seed=42,
        n_customers=100000,
        n_categories=12,
        n_products=2000,
        n_orders=500000,
        max_items_per_order=10,
        n_inventory_days=360,
        n_sessions=1200000,
        start_date="2025-01-01",
        end_date=str(date.today()),
    )

    generator = WebshopDataGenerator(config)
    tables = generator.run()

    print("\nDone. Generated tables:")
    for name, df in tables.items():
        print(f"- {name}: {len(df):,} rows")


if __name__ == "__main__":
    main()
