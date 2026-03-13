from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import supertable.config.homedir
import numpy as np
import pandas as pd
from tqdm import tqdm


@dataclass
class GenerationConfig:
    output_dir: str = "synthetic_webshop_data"
    seed: int = 42
    n_customers: int = 5000
    n_categories: int = 12
    n_products: int = 1200
    n_orders: int = 20000
    max_items_per_order: int = 6
    n_inventory_days: int = 120
    n_sessions: int = 50000
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"


class WebshopDataGenerator:
    def __init__(self, config: GenerationConfig):
        self.config = config
        self.rng = np.random.default_rng(config.seed)
        random.seed(config.seed)

        try:
            self.start_date = pd.Timestamp(config.start_date)
            self.end_date = pd.Timestamp(config.end_date)
        except Exception as exc:
            raise ValueError(
                f"Invalid date in config. start_date={config.start_date}, end_date={config.end_date}"
            ) from exc

        if self.end_date <= self.start_date:
            raise ValueError("end_date must be later than start_date")

        self.brand_pool = [
            "Nova", "Aero", "UrbanNest", "Zenith", "Pulse", "Luma", "Atlas",
            "EverPeak", "Cobalt", "Velora", "Northbay", "Solis", "Nimbus", "Axiom",
        ]
        self.country_pool = [
            "Hungary", "Germany", "Austria", "Romania", "Slovakia", "Poland", "Czechia",
        ]
        self.city_map = {
            "Hungary": ["Budapest", "Debrecen", "Szeged", "Pécs", "Győr"],
            "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
            "Austria": ["Vienna", "Graz", "Linz", "Salzburg"],
            "Romania": ["Cluj-Napoca", "Bucharest", "Timișoara", "Iași"],
            "Slovakia": ["Bratislava", "Košice", "Žilina"],
            "Poland": ["Warsaw", "Kraków", "Wrocław", "Gdańsk"],
            "Czechia": ["Prague", "Brno", "Ostrava"],
        }
        self.device_types = ["mobile", "desktop", "tablet"]
        self.channels = ["organic", "paid_search", "social", "email", "direct", "affiliate"]
        self.payment_methods = [
            "card", "paypal", "bank_transfer", "cash_on_delivery", "apple_pay", "google_pay",
        ]
        self.order_statuses = [
            "delivered", "delivered", "delivered", "shipped", "processing", "cancelled", "returned",
        ]

    def run(self) -> Dict[str, pd.DataFrame]:
        print("\nStarting dataset generation...\n")

        print("Generating categories...")
        categories = self.generate_categories()

        print("Generating products...")
        products = self.generate_products(categories)

        print("Generating customers...")
        customers = self.generate_customers()

        print("Generating orders and order items...")
        orders, order_items = self.generate_orders_and_items(customers, products)

        print("Generating inventory snapshots...")
        inventory = self.generate_inventory_snapshots(products)

        print("Generating sessions and pageviews...")
        sessions, pageviews = self.generate_sessions_and_pageviews(customers, products)

        print("Generating product daily stats...")
        product_daily_stats = self.generate_product_daily_stats(products, pageviews, order_items, orders)

        tables = {
            "categories": categories,
            "products": products,
            "customers": customers,
            "orders": orders,
            "order_items": order_items,
            "inventory_snapshots": inventory,
            "sessions": sessions,
            "pageviews": pageviews,
            "product_daily_stats": product_daily_stats,
        }

        print("Writing parquet files...")
        self.write_tables(tables)
        return tables

    def generate_categories(self) -> pd.DataFrame:
        category_names = [
            "Electronics", "Home & Kitchen", "Fashion", "Beauty", "Sports", "Toys",
            "Books", "Office", "Garden", "Pet Supplies", "Health", "Automotive",
        ]
        category_names = category_names[: self.config.n_categories]

        rows = []
        for i, name in enumerate(category_names, start=1):
            rows.append(
                {
                    "category_id": i,
                    "category_name": name,
                    "department": name.split(" & ")[0],
                    "is_active": True,
                    "created_at": self.start_date - pd.Timedelta(days=int(self.rng.integers(30, 600))),
                }
            )
        return pd.DataFrame(rows)

    def generate_products(self, categories: pd.DataFrame) -> pd.DataFrame:
        product_terms = {
            "Electronics": ["Headphones", "Monitor", "Keyboard", "Mouse", "Webcam", "Speaker", "Tablet", "Smartwatch"],
            "Home & Kitchen": ["Blender", "Knife Set", "Air Fryer", "Coffee Maker", "Lamp", "Storage Box", "Cookware Set"],
            "Fashion": ["Sneakers", "Jacket", "Backpack", "Jeans", "T-Shirt", "Dress", "Boots"],
            "Beauty": ["Serum", "Moisturizer", "Perfume", "Shampoo", "Face Mask", "Cleanser"],
            "Sports": ["Yoga Mat", "Dumbbell", "Running Belt", "Cycling Bottle", "Resistance Band", "Tennis Bag"],
            "Toys": ["Puzzle", "Building Set", "Toy Car", "Board Game", "Doll", "Science Kit"],
            "Books": ["Novel", "Cookbook", "Workbook", "Planner", "Guide", "Album"],
            "Office": ["Desk Chair", "Notebook", "Pen Set", "Standing Desk", "Paper Tray", "Mouse Pad"],
            "Garden": ["Plant Pot", "Watering Can", "Garden Shears", "Seed Pack", "Outdoor Light"],
            "Pet Supplies": ["Dog Bed", "Cat Toy", "Leash", "Feeder", "Scratching Post", "Pet Shampoo"],
            "Health": ["Vitamin Box", "Massager", "Thermometer", "Pill Organizer", "Fitness Scale"],
            "Automotive": ["Phone Mount", "Car Vacuum", "Seat Cover", "Dash Cam", "Air Freshener"],
        }

        rows = []
        category_ids = categories["category_id"].tolist()
        category_names = dict(zip(categories["category_id"], categories["category_name"]))

        for product_id in tqdm(
            range(1, self.config.n_products + 1),
            desc="Generating products",
            unit="products",
            dynamic_ncols=True,
            leave=False,
        ):
            category_id = int(self.rng.choice(category_ids))
            category_name = category_names[category_id]
            term = random.choice(product_terms.get(category_name, ["Item"]))
            brand = random.choice(self.brand_pool)
            adjective = random.choice(["Pro", "Lite", "Plus", "Max", "Prime", "Core", "Eco", "Elite"])
            base_price = self.price_by_category(category_name)
            price = round(max(5.0, self.rng.normal(base_price, base_price * 0.28)), 2)
            cost = round(price * float(self.rng.uniform(0.45, 0.78)), 2)
            rating = round(float(np.clip(self.rng.normal(4.2, 0.45), 2.5, 5.0)), 2)
            review_count = int(max(0, self.rng.lognormal(mean=3.4, sigma=0.85)))
            created_at = self.random_timestamp(
                self.start_date - pd.Timedelta(days=300),
                self.end_date - pd.Timedelta(days=10),
            )

            rows.append(
                {
                    "product_id": product_id,
                    "sku": f"SKU-{product_id:06d}",
                    "product_name": f"{brand} {term} {adjective}",
                    "brand": brand,
                    "category_id": category_id,
                    "price": price,
                    "cost": cost,
                    "margin_pct": round((price - cost) / price, 4),
                    "rating": rating,
                    "review_count": review_count,
                    "is_active": bool(self.rng.random() > 0.03),
                    "weight_kg": round(float(np.clip(self.rng.normal(1.6, 1.1), 0.1, 15.0)), 2),
                    "color": random.choice(["black", "white", "blue", "red", "green", "grey", "beige"]),
                    "created_at": created_at,
                }
            )

        return pd.DataFrame(rows)

    def generate_customers(self) -> pd.DataFrame:
        first_names = [
            "Adam", "Ben", "Clara", "Dora", "Eva", "Felix", "Gabor", "Hanna", "Ivan", "Julia",
            "Kinga", "Luca", "Marta", "Noel", "Olivia", "Peter", "Rita", "Sara", "Tamas", "Zoe",
        ]
        last_names = [
            "Nagy", "Kovacs", "Szabo", "Toth", "Varga", "Kiss", "Farkas", "Molnar", "Balogh", "Papp",
        ]

        rows = []
        for customer_id in tqdm(
            range(1, self.config.n_customers + 1),
            desc="Generating customers",
            unit="customers",
            dynamic_ncols=True,
            leave=False,
        ):
            country = random.choice(self.country_pool)
            city = random.choice(self.city_map[country])
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            created_at = self.random_timestamp(
                self.start_date - pd.Timedelta(days=500),
                self.end_date - pd.Timedelta(days=1),
            )
            birth_year = int(self.rng.integers(1958, 2008))

            rows.append(
                {
                    "customer_id": customer_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": f"{first_name.lower()}.{last_name.lower()}{customer_id}@example.com",
                    "country": country,
                    "city": city,
                    "gender": random.choice(["female", "male", "other"]),
                    "birth_year": birth_year,
                    "customer_segment": random.choice(["new", "returning", "vip", "bargain_hunter"]),
                    "marketing_opt_in": bool(self.rng.random() > 0.22),
                    "created_at": created_at,
                }
            )
        return pd.DataFrame(rows)

    def generate_orders_and_items(
        self,
        customers: pd.DataFrame,
        products: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        product_ids = products["product_id"].to_numpy()
        product_price = products.set_index("product_id")["price"].to_dict()
        product_cost = products.set_index("product_id")["cost"].to_dict()
        customer_ids = customers["customer_id"].to_numpy()

        orders_rows: List[dict] = []
        item_rows: List[dict] = []
        item_id = 1

        popularity_weights = self.product_popularity_weights(products)

        for order_id in tqdm(
            range(1, self.config.n_orders + 1),
            desc="Generating orders",
            unit="orders",
            dynamic_ncols=True,
        ):
            customer_id = int(self.rng.choice(customer_ids))
            order_ts = self.weighted_order_timestamp()
            status = random.choice(self.order_statuses)
            payment_method = random.choice(self.payment_methods)
            channel = random.choice(self.channels)
            device_type = random.choices(self.device_types, weights=[0.58, 0.33, 0.09], k=1)[0]
            item_count = int(self.rng.integers(1, self.config.max_items_per_order + 1))

            chosen_products = self.rng.choice(
                product_ids,
                size=item_count,
                replace=False,
                p=popularity_weights,
            )
            order_subtotal = 0.0
            order_cost_total = 0.0
            units_total = 0

            for product_id in chosen_products:
                qty = int(self.rng.choice([1, 1, 1, 2, 2, 3, 4]))
                unit_price = float(product_price[int(product_id)])
                discount_pct = float(np.clip(self.rng.normal(0.08, 0.07), 0.0, 0.35))

                if status == "cancelled":
                    qty = 0
                    discount_pct = 0.0

                net_unit_price = round(unit_price * (1 - discount_pct), 2)
                line_revenue = round(qty * net_unit_price, 2)
                line_cost = round(qty * float(product_cost[int(product_id)]), 2)
                order_subtotal += line_revenue
                order_cost_total += line_cost
                units_total += qty

                item_rows.append(
                    {
                        "order_item_id": item_id,
                        "order_id": order_id,
                        "product_id": int(product_id),
                        "quantity": qty,
                        "unit_price": round(unit_price, 2),
                        "discount_pct": round(discount_pct, 4),
                        "net_unit_price": net_unit_price,
                        "line_revenue": line_revenue,
                        "line_cost": line_cost,
                        "returned_qty": int(qty if status == "returned" and self.rng.random() > 0.4 else 0),
                    }
                )
                item_id += 1

            shipping_fee = 0.0 if order_subtotal >= 80 else round(
                float(self.rng.choice([3.99, 4.99, 5.99, 6.99])), 2
            )
            tax_amount = round(order_subtotal * 0.27, 2)
            order_total = round(order_subtotal + shipping_fee + tax_amount, 2)
            gross_margin = round(order_subtotal - order_cost_total, 2)

            estimated_delivery_days = None
            delivered_at = None
            if status in {"delivered", "returned", "shipped"}:
                estimated_delivery_days = int(self.rng.integers(1, 8))
            if status in {"delivered", "returned"}:
                delivered_at = order_ts + pd.Timedelta(days=estimated_delivery_days)

            orders_rows.append(
                {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_timestamp": order_ts,
                    "order_date": order_ts.normalize(),
                    "status": status,
                    "payment_method": payment_method,
                    "channel": channel,
                    "device_type": device_type,
                    "items_count": item_count,
                    "units_total": units_total,
                    "subtotal": round(order_subtotal, 2),
                    "shipping_fee": shipping_fee,
                    "tax_amount": tax_amount,
                    "order_total": order_total,
                    "gross_margin": gross_margin,
                    "coupon_code": random.choice([None, None, None, "WELCOME10", "SPRING15", "VIP20"]),
                    "estimated_delivery_days": estimated_delivery_days,
                    "delivered_at": delivered_at,
                }
            )

        return pd.DataFrame(orders_rows), pd.DataFrame(item_rows)

    def generate_inventory_snapshots(self, products: pd.DataFrame) -> pd.DataFrame:
        days = pd.date_range(
            self.end_date - pd.Timedelta(days=self.config.n_inventory_days - 1),
            self.end_date,
            freq="D",
        )
        rows = []

        for _, product in tqdm(
            products.iterrows(),
            total=len(products),
            desc="Generating inventory",
            unit="products",
            dynamic_ncols=True,
        ):
            stock = int(max(0, self.rng.normal(80, 40)))
            warehouse = random.choice(["WH-BUD-01", "WH-BUD-02", "WH-DEB-01"])

            for day in days:
                inbound = int(max(0, self.rng.poisson(1.2) if self.rng.random() > 0.82 else 0))
                outbound = int(max(0, self.rng.poisson(1.8)))
                stock = max(0, stock + inbound - outbound)

                rows.append(
                    {
                        "snapshot_date": day,
                        "product_id": int(product["product_id"]),
                        "warehouse_code": warehouse,
                        "stock_on_hand": stock,
                        "inbound_units": inbound,
                        "outbound_units": outbound,
                        "is_low_stock": stock < 10,
                    }
                )
        return pd.DataFrame(rows)

    def generate_sessions_and_pageviews(
        self,
        customers: pd.DataFrame,
        products: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        customer_ids = customers["customer_id"].to_numpy()
        product_ids = products["product_id"].to_numpy()
        popularity_weights = self.product_popularity_weights(products)
        sessions_rows = []
        pageviews_rows = []
        pageview_id = 1

        customer_choice_array = np.append(customer_ids, [0])
        customer_choice_weights = self.customer_session_weights(len(customer_ids))

        for session_id in tqdm(
            range(1, self.config.n_sessions + 1),
            desc="Generating sessions",
            unit="sessions",
            dynamic_ncols=True,
        ):
            session_ts = self.weighted_order_timestamp()
            customer_id = int(self.rng.choice(customer_choice_array, p=customer_choice_weights))
            device_type = random.choices(self.device_types, weights=[0.62, 0.29, 0.09], k=1)[0]
            channel = random.choice(self.channels)
            page_count = int(self.rng.integers(1, 12))
            duration_seconds = int(max(10, self.rng.normal(210, 110)))
            converted = bool(self.rng.random() < 0.11)
            bounced = page_count == 1 and duration_seconds < 30

            sessions_rows.append(
                {
                    "session_id": session_id,
                    "customer_id": None if customer_id == 0 else customer_id,
                    "session_start": session_ts,
                    "channel": channel,
                    "device_type": device_type,
                    "page_count": page_count,
                    "duration_seconds": duration_seconds,
                    "converted": converted,
                    "bounced": bounced,
                }
            )

            viewed_products = self.rng.choice(
                product_ids,
                size=min(page_count, 8),
                replace=False,
                p=popularity_weights,
            )

            for position, product_id in enumerate(viewed_products, start=1):
                add_to_cart = bool(self.rng.random() < 0.18)
                purchased = bool(converted and add_to_cart and self.rng.random() < 0.55)

                pageviews_rows.append(
                    {
                        "pageview_id": pageview_id,
                        "session_id": session_id,
                        "customer_id": None if customer_id == 0 else customer_id,
                        "product_id": int(product_id),
                        "view_timestamp": session_ts + pd.Timedelta(
                            seconds=position * int(self.rng.integers(15, 70))
                        ),
                        "page_position": position,
                        "add_to_cart": add_to_cart,
                        "purchased": purchased,
                        "dwell_seconds": int(max(5, self.rng.normal(36, 18))),
                    }
                )
                pageview_id += 1

        return pd.DataFrame(sessions_rows), pd.DataFrame(pageviews_rows)

    def generate_product_daily_stats(
        self,
        products: pd.DataFrame,
        pageviews: pd.DataFrame,
        order_items: pd.DataFrame,
        orders: pd.DataFrame,
    ) -> pd.DataFrame:
        pageviews = pageviews.copy()
        pageviews["stat_date"] = pd.to_datetime(pageviews["view_timestamp"]).dt.normalize()

        orders_lookup = orders[["order_id", "order_date", "status"]].copy()
        valid_items = order_items.merge(orders_lookup, on="order_id", how="left")
        valid_items = valid_items[
            valid_items["status"].isin(["delivered", "shipped", "processing", "returned"])
        ]

        views_agg = (
            pageviews.groupby(["stat_date", "product_id"], as_index=False)
            .agg(
                views=("pageview_id", "count"),
                add_to_cart_count=("add_to_cart", "sum"),
                purchase_clicks=("purchased", "sum"),
                avg_dwell_seconds=("dwell_seconds", "mean"),
            )
        )

        sales_agg = (
            valid_items.groupby(["order_date", "product_id"], as_index=False)
            .agg(
                units_sold=("quantity", "sum"),
                revenue=("line_revenue", "sum"),
                returned_units=("returned_qty", "sum"),
            )
            .rename(columns={"order_date": "stat_date"})
        )

        merged = views_agg.merge(sales_agg, on=["stat_date", "product_id"], how="outer")
        merged = merged.merge(
            products[["product_id", "price", "rating", "review_count"]],
            on="product_id",
            how="left",
        )

        for col in ["views", "add_to_cart_count", "purchase_clicks", "units_sold", "returned_units"]:
            merged[col] = merged[col].fillna(0).astype(int)
        for col in ["revenue", "avg_dwell_seconds"]:
            merged[col] = merged[col].fillna(0.0)

        merged["conversion_rate"] = np.where(
            merged["views"] > 0, merged["units_sold"] / merged["views"], 0.0
        )
        merged["add_to_cart_rate"] = np.where(
            merged["views"] > 0, merged["add_to_cart_count"] / merged["views"], 0.0
        )
        merged["return_rate"] = np.where(
            merged["units_sold"] > 0, merged["returned_units"] / merged["units_sold"], 0.0
        )
        merged["revenue"] = merged["revenue"].round(2)
        merged["avg_dwell_seconds"] = merged["avg_dwell_seconds"].round(2)
        merged["conversion_rate"] = merged["conversion_rate"].round(4)
        merged["add_to_cart_rate"] = merged["add_to_cart_rate"].round(4)
        merged["return_rate"] = merged["return_rate"].round(4)

        return merged.sort_values(["stat_date", "product_id"]).reset_index(drop=True)

    def write_tables(self, tables: Dict[str, pd.DataFrame]) -> None:
        output_root = Path(self.config.output_dir)
        output_root.mkdir(parents=True, exist_ok=True)

        for table_name, df in tables.items():
            table_dir = output_root / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            file_path = table_dir / f"{table_name}.parquet"
            df.to_parquet(file_path, index=False)
            print(f"Wrote {table_name:<22} -> {file_path} ({len(df):,} rows)")

    def random_timestamp(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.Timestamp:
        total_seconds = int((end - start).total_seconds())
        offset = int(self.rng.integers(0, max(total_seconds, 1)))
        return start + pd.Timedelta(seconds=offset)

    def weighted_order_timestamp(self) -> pd.Timestamp:
        total_days = (self.end_date - self.start_date).days
        day_offset = int(self.rng.integers(0, total_days + 1))
        base_date = self.start_date + pd.Timedelta(days=day_offset)

        if base_date.month in [11, 12]:
            base_date += pd.Timedelta(hours=int(self.rng.integers(0, 8)))
        if base_date.day in [1, 15, 28]:
            base_date += pd.Timedelta(hours=int(self.rng.integers(0, 5)))

        hour = int(np.clip(self.rng.normal(14, 5), 0, 23))
        minute = int(self.rng.integers(0, 60))
        second = int(self.rng.integers(0, 60))
        return pd.Timestamp(base_date.year, base_date.month, base_date.day, hour, minute, second)

    def price_by_category(self, category_name: str) -> float:
        return {
            "Electronics": 140,
            "Home & Kitchen": 65,
            "Fashion": 55,
            "Beauty": 28,
            "Sports": 48,
            "Toys": 32,
            "Books": 18,
            "Office": 42,
            "Garden": 36,
            "Pet Supplies": 30,
            "Health": 38,
            "Automotive": 44,
        }.get(category_name, 35)

    def product_popularity_weights(self, products: pd.DataFrame) -> np.ndarray:
        base = np.linspace(1.0, 3.5, len(products))
        self.rng.shuffle(base)
        return base / base.sum()

    def customer_session_weights(self, n_customers: int) -> np.ndarray:
        guest_weight = 0.42
        customer_weight = (1 - guest_weight) / n_customers
        weights = np.full(n_customers + 1, customer_weight, dtype=float)
        weights[-1] = guest_weight
        return weights


def parse_args() -> GenerationConfig:
    parser = argparse.ArgumentParser(description="Generate synthetic webshop parquet datasets")
    parser.add_argument("--output-dir", default="synthetic_webshop_data")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--categories", type=int, default=12)
    parser.add_argument("--products", type=int, default=1200)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--max-items-per-order", type=int, default=6)
    parser.add_argument("--inventory-days", type=int, default=120)
    parser.add_argument("--sessions", type=int, default=50000)
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    args = parser.parse_args()

    return GenerationConfig(
        output_dir=args.output_dir,
        seed=args.seed,
        n_customers=args.customers,
        n_categories=args.categories,
        n_products=args.products,
        n_orders=args.orders,
        max_items_per_order=args.max_items_per_order,
        n_inventory_days=args.inventory_days,
        n_sessions=args.sessions,
        start_date=args.start_date,
        end_date=args.end_date,
    )


def main() -> None:
    config = parse_args()
    generator = WebshopDataGenerator(config)
    tables = generator.run()

    print("\nDone. Generated tables:")
    for name, df in tables.items():
        print(f"- {name}: {len(df):,} rows")


if __name__ == "__main__":
    main()
