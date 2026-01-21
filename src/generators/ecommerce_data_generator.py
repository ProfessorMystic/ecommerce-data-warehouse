"""
E-commerce Data Generator Module

Generates synthetic e-commerce data for data warehouse development and testing.
Uses the Faker library to create realistic customer, product, and order data.

Usage:
    from src.generators.ecommerce_data_generator import EcommerceDataGenerator

    generator = EcommerceDataGenerator()
    data = generator.generate_all(n_customers=1000, n_products=200, n_orders=5000)

Output Files:
    - customers.csv: Customer profiles with demographics and segments
    - products.csv: Product catalog with pricing and inventory
    - categories.csv: Product category reference data
    - orders.csv: Order headers with totals and status
    - order_items.csv: Order line items with quantities and pricing
"""

import os
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd

# Initialize Faker with seed for reproducible data generation
# This ensures the same "random" data is generated each run, useful for testing
fake = Faker()
Faker.seed(42)
random.seed(42)


class EcommerceDataGenerator:
    """
    Generates realistic e-commerce dataset for data warehouse modeling.

    Creates interconnected data mimicking a real online retail business:
    - Customers with segments (premium, regular, occasional, new)
    - Products across 8 categories with realistic pricing
    - Orders with multiple line items, discounts, and shipping

    Attributes:
        categories: List of product categories with profit margins
        product_templates: Product name templates by category
        order_statuses: Possible order status values (weighted toward completed)
    """

    def __init__(self):
        # Product categories with typical profit margins
        # Margins used to calculate cost from price (cost = price * (1 - margin))
        self.categories = [
            {"id": 1, "name": "Electronics", "margin": 0.15},
            {"id": 2, "name": "Clothing", "margin": 0.40},
            {"id": 3, "name": "Home & Garden", "margin": 0.35},
            {"id": 4, "name": "Sports", "margin": 0.30},
            {"id": 5, "name": "Books", "margin": 0.25},
            {"id": 6, "name": "Toys", "margin": 0.45},
            {"id": 7, "name": "Beauty", "margin": 0.50},
            {"id": 8, "name": "Food & Grocery", "margin": 0.20},
        ]

        # Base product names by category - combined with Faker words for variety
        self.product_templates = {
            "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Smart Watch", "Camera", "Speaker",
                            "Monitor"],
            "Clothing": ["T-Shirt", "Jeans", "Dress", "Jacket", "Sneakers", "Hoodie", "Shorts", "Sweater"],
            "Home & Garden": ["Lamp", "Rug", "Plant Pot", "Cushion", "Blanket", "Vase", "Mirror", "Clock"],
            "Sports": ["Yoga Mat", "Dumbbells", "Running Shoes", "Basketball", "Tennis Racket", "Bike Helmet",
                       "Gym Bag", "Water Bottle"],
            "Books": ["Fiction Novel", "Cookbook", "Biography", "Self-Help Book", "Science Book", "History Book",
                      "Art Book", "Travel Guide"],
            "Toys": ["Board Game", "Puzzle", "Action Figure", "Doll", "Building Blocks", "Remote Control Car",
                     "Stuffed Animal", "Card Game"],
            "Beauty": ["Moisturizer", "Lipstick", "Perfume", "Shampoo", "Face Mask", "Nail Polish", "Sunscreen",
                       "Hair Dryer"],
            "Food & Grocery": ["Coffee Beans", "Olive Oil", "Chocolate Box", "Spice Set", "Tea Collection", "Honey",
                               "Pasta Set", "Snack Box"],
        }

        # Order statuses weighted toward completed (realistic distribution)
        # 50% completed, rest distributed among other statuses
        self.order_statuses = ["completed", "completed", "completed", "completed", "shipped", "processing", "cancelled",
                               "returned"]

    def generate_customers(self, n: int = 1000) -> pd.DataFrame:
        """
        Generate customer records with realistic demographics and behavior segments.

        Segments influence purchasing behavior:
        - premium (10%): Long-time customers, higher order frequency
        - regular (40%): Consistent buyers
        - occasional (30%): Infrequent purchases
        - new (20%): Recently registered

        Args:
            n: Number of customers to generate

        Returns:
            DataFrame with customer records including PII and segment data
        """
        customers = []

        # Segment distribution reflects typical e-commerce customer base
        segments = ["premium", "regular", "occasional", "new"]
        segment_weights = [0.1, 0.4, 0.3, 0.2]

        for i in range(1, n + 1):
            segment = random.choices(segments, segment_weights)[0]

            # Registration date varies by segment to create realistic tenure
            if segment == "new":
                reg_date = fake.date_between(start_date="-3m", end_date="today")
            elif segment == "premium":
                reg_date = fake.date_between(start_date="-3y", end_date="-1y")
            else:
                reg_date = fake.date_between(start_date="-2y", end_date="-1m")

            customer = {
                "customer_id": i,
                "email": fake.email(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "phone": fake.phone_number(),
                "address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip_code": fake.zipcode(),
                "country": "USA",
                "segment": segment,
                "registration_date": reg_date,
                "is_active": random.choices([True, False], [0.9, 0.1])[0],  # 90% active
                "created_at": datetime.combine(reg_date, datetime.min.time()),
                "updated_at": datetime.now(),
            }
            customers.append(customer)

        return pd.DataFrame(customers)

    def generate_products(self, n: int = 200) -> pd.DataFrame:
        """
        Generate product catalog with category-appropriate pricing.

        Pricing logic:
        - Electronics: $50-1500 (high-value items)
        - Books: $10-50 (lower price point)
        - Other categories: $15-200

        Cost calculated from price using category margin.

        Args:
            n: Number of products to generate

        Returns:
            DataFrame with product records including SKU, pricing, and inventory
        """
        products = []

        for i in range(1, n + 1):
            category = random.choice(self.categories)
            category_name = category["name"]
            base_product = random.choice(self.product_templates[category_name])

            # Category-specific price ranges for realism
            if category_name == "Electronics":
                price = round(random.uniform(50, 1500), 2)
            elif category_name == "Books":
                price = round(random.uniform(10, 50), 2)
            else:
                price = round(random.uniform(15, 200), 2)

            # Cost derived from price and category margin
            cost = round(price * (1 - category["margin"]), 2)

            product = {
                "product_id": i,
                "sku": f"SKU-{category['id']:02d}-{i:05d}",  # Format: SKU-CC-NNNNN
                "name": f"{fake.word().title()} {base_product}",
                "description": fake.sentence(nb_words=10),
                "category_id": category["id"],
                "category_name": category_name,
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(0, 500),
                "is_active": random.choices([True, False], [0.85, 0.15])[0],  # 85% active
                "created_at": fake.date_time_between(start_date="-2y", end_date="-6m"),
                "updated_at": datetime.now(),
            }
            products.append(product)

        return pd.DataFrame(products)

    def generate_orders(self, customers_df: pd.DataFrame, products_df: pd.DataFrame,
                        n_orders: int = 5000) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate orders and associated line items.

        Business logic:
        - Premium customers are 4x more likely to have orders
        - Premium customers order 2-6 items; others order 1-4
        - 20% of items receive a discount (10%, 15%, or 20% off)
        - Free shipping on orders over $50
        - 8% tax rate applied to subtotal

        Args:
            customers_df: Customer DataFrame (must include customer_id, segment, registration_date)
            products_df: Product DataFrame (must include product_id, price, is_active)
            n_orders: Number of orders to generate

        Returns:
            Tuple of (orders_df, order_items_df)
        """
        orders = []
        order_items = []
        order_item_id = 1

        # Only sell active products
        active_products = products_df[products_df["is_active"] == True]

        for order_id in range(1, n_orders + 1):
            # Weight customer selection by segment (premium customers order more)
            customer = customers_df.sample(
                weights=customers_df["segment"].map({
                    "premium": 4, "regular": 2, "occasional": 1, "new": 1
                })
            ).iloc[0]

            # Orders must occur after customer registration
            min_date = max(customer["registration_date"], datetime(2023, 1, 1).date())
            order_date = fake.date_time_between(
                start_date=min_date,
                end_date="now"
            )

            status = random.choice(self.order_statuses)

            # Premium customers tend to buy more items per order
            if customer["segment"] == "premium":
                n_items = random.randint(2, 6)
            else:
                n_items = random.randint(1, 4)

            # Select random products for this order
            order_products = active_products.sample(n=min(n_items, len(active_products)))

            # Build order items and calculate subtotal
            subtotal = 0
            for _, product in order_products.iterrows():
                quantity = random.randint(1, 3)
                unit_price = product["price"]

                # Apply occasional discount (20% chance)
                discount = 0
                if random.random() < 0.2:
                    discount = round(unit_price * random.choice([0.1, 0.15, 0.2]), 2)

                line_total = round((unit_price - discount) * quantity, 2)
                subtotal += line_total

                order_item = {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": discount,
                    "line_total": line_total,
                }
                order_items.append(order_item)
                order_item_id += 1

            # Calculate order totals
            # Free shipping threshold: $50
            shipping = round(random.choice([0, 5.99, 9.99, 14.99]), 2) if subtotal < 50 else 0
            tax = round(subtotal * 0.08, 2)  # 8% tax rate
            total = round(subtotal + shipping + tax, 2)

            order = {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_date": order_date,
                "status": status,
                "subtotal": round(subtotal, 2),
                "shipping": shipping,
                "tax": tax,
                "total": total,
                "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
                "shipping_address": customer["address"],
                "shipping_city": customer["city"],
                "shipping_state": customer["state"],
                "shipping_zip": customer["zip_code"],
                "created_at": order_date,
                "updated_at": order_date + timedelta(days=random.randint(0, 5)),
            }
            orders.append(order)

        return pd.DataFrame(orders), pd.DataFrame(order_items)

    def generate_all(self, n_customers: int = 1000, n_products: int = 200,
                     n_orders: int = 5000, output_dir: str = "data/generated") -> dict:
        """
        Generate complete e-commerce dataset and save to CSV files.

        Creates all interconnected tables needed for a data warehouse:
        customers, products, categories, orders, and order_items.

        Args:
            n_customers: Number of customer records
            n_products: Number of product records
            n_orders: Number of order records (line items auto-generated)
            output_dir: Directory path for CSV output

        Returns:
            Dictionary containing all generated DataFrames
        """
        os.makedirs(output_dir, exist_ok=True)

        print("Generating customers...")
        customers_df = self.generate_customers(n_customers)
        customers_df.to_csv(f"{output_dir}/customers.csv", index=False)
        print(f"  Created {len(customers_df)} customers")

        print("Generating products...")
        products_df = self.generate_products(n_products)
        products_df.to_csv(f"{output_dir}/products.csv", index=False)
        print(f"  Created {len(products_df)} products")

        # Save categories as reference table
        categories_df = pd.DataFrame(self.categories)
        categories_df.to_csv(f"{output_dir}/categories.csv", index=False)
        print(f"  Created {len(categories_df)} categories")

        print("Generating orders...")
        orders_df, order_items_df = self.generate_orders(customers_df, products_df, n_orders)
        orders_df.to_csv(f"{output_dir}/orders.csv", index=False)
        order_items_df.to_csv(f"{output_dir}/order_items.csv", index=False)
        print(f"  Created {len(orders_df)} orders with {len(order_items_df)} line items")

        print("-" * 50)
        print("Data generation complete!")
        print(f"Files saved to: {output_dir}/")

        return {
            "customers": customers_df,
            "products": products_df,
            "categories": categories_df,
            "orders": orders_df,
            "order_items": order_items_df,
        }


if __name__ == "__main__":
    generator = EcommerceDataGenerator()
    data = generator.generate_all()