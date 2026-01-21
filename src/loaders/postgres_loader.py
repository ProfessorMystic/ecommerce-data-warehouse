"""
PostgreSQL Data Loader Module

Loads generated CSV data into PostgreSQL staging tables.
Creates schema and tables automatically, then bulk loads data.

Usage:
    from src.loaders.postgres_loader import PostgresLoader

    loader = PostgresLoader()
    loader.load_all()
"""

import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


class PostgresLoader:
    """
    Loads e-commerce data from CSV files into PostgreSQL.

    Creates staging schema and tables, then performs bulk inserts.
    Designed for initial loads; truncates tables before each load.
    """

    def __init__(self):
        """Initialize database connection using environment variables."""
        self.conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
            dbname=os.getenv("PG_DATABASE", "ecommerce_dw"),
            user=os.getenv("PG_USER", os.getenv("USER")),
            password=os.getenv("PG_PASSWORD", "")
        )
        print("Connected to PostgreSQL")

    def create_schemas(self):
        """
        Create database schemas for data warehouse layers.

        Schemas:
        - staging: Raw data landing zone
        - warehouse: Dimensional model (facts and dimensions)
        - analytics: dbt marts for reporting
        """
        schemas = ["staging", "warehouse", "analytics"]

        with self.conn.cursor() as cur:
            for schema in schemas:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        self.conn.commit()
        print(f"Created schemas: {', '.join(schemas)}")

    def create_staging_tables(self):
        """Create staging tables matching CSV structure."""

        # Staging tables mirror source data with minimal transformation
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS staging.customers (
                customer_id INTEGER PRIMARY KEY,
                email VARCHAR(255),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                phone VARCHAR(50),
                address VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(10),
                zip_code VARCHAR(20),
                country VARCHAR(50),
                segment VARCHAR(50),
                registration_date DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.products (
                product_id INTEGER PRIMARY KEY,
                sku VARCHAR(50),
                name VARCHAR(255),
                description TEXT,
                category_id INTEGER,
                category_name VARCHAR(100),
                price DECIMAL(10,2),
                cost DECIMAL(10,2),
                stock_quantity INTEGER,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                margin DECIMAL(5,2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date TIMESTAMP,
                status VARCHAR(50),
                subtotal DECIMAL(10,2),
                shipping DECIMAL(10,2),
                tax DECIMAL(10,2),
                total DECIMAL(10,2),
                payment_method VARCHAR(50),
                shipping_address VARCHAR(255),
                shipping_city VARCHAR(100),
                shipping_state VARCHAR(10),
                shipping_zip VARCHAR(20),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.order_items (
                order_item_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                discount DECIMAL(10,2),
                line_total DECIMAL(10,2)
            )
            """
        ]

        with self.conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)

        self.conn.commit()
        print("Created staging tables")

    def _convert_value(self, val):
        """Convert numpy/pandas types to native Python types for PostgreSQL."""
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        elif isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        elif isinstance(val, (np.floating, np.float64, np.float32)):
            return float(val)
        elif isinstance(val, np.bool_):
            return bool(val)
        elif isinstance(val, pd.Timestamp):
            return val.to_pydatetime()
        else:
            return val

    def load_csv_to_table(self, csv_path: str, schema: str, table: str):
        """
        Load CSV file into specified table using bulk insert.

        Truncates existing data before loading (full refresh pattern).

        Args:
            csv_path: Path to CSV file
            schema: Database schema name
            table: Table name
        """
        df = pd.read_csv(csv_path)

        columns = df.columns.tolist()

        # Convert all values to native Python types
        values = [
            tuple(self._convert_value(val) for val in row)
            for row in df.values
        ]

        with self.conn.cursor() as cur:
            # Truncate for full refresh
            cur.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE")

            # Bulk insert using execute_values for performance
            insert_sql = f"""
                INSERT INTO {schema}.{table} ({', '.join(columns)})
                VALUES %s
            """
            execute_values(cur, insert_sql, values)

        self.conn.commit()
        print(f"  Loaded {len(df)} rows into {schema}.{table}")

    def load_all(self, data_dir: str = "data/generated"):
        """
        Load all CSV files into staging tables.

        Args:
            data_dir: Directory containing generated CSV files
        """
        print("Setting up database...")
        self.create_schemas()
        self.create_staging_tables()

        print("\nLoading data into staging tables...")

        # Map CSV files to tables
        files_to_tables = {
            "customers.csv": "customers",
            "products.csv": "products",
            "categories.csv": "categories",
            "orders.csv": "orders",
            "order_items.csv": "order_items",
        }

        for filename, table in files_to_tables.items():
            csv_path = os.path.join(data_dir, filename)
            if os.path.exists(csv_path):
                self.load_csv_to_table(csv_path, "staging", table)
            else:
                print(f"  Warning: {csv_path} not found")

        print("\nData load complete!")

    def verify_load(self):
        """Print row counts for all staging tables."""
        tables = ["customers", "products", "categories", "orders", "order_items"]

        print("\nStaging table row counts:")
        with self.conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
                count = cur.fetchone()[0]
                print(f"  staging.{table}: {count:,} rows")

    def close(self):
        """Close database connection."""
        self.conn.close()
        print("\nConnection closed")


if __name__ == "__main__":
    loader = PostgresLoader()
    loader.load_all()
    loader.verify_load()
    loader.close()