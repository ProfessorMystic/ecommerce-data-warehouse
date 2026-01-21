# E-Commerce Data Warehouse

A production-ready data warehouse built with dbt and PostgreSQL, implementing dimensional modeling for e-commerce analytics.

## Architecture
```
Source Data (CSV) → Python Loader → PostgreSQL → dbt → Analytics
```

**Data Layers:**
- **Staging**: Light transformations on raw data (views)
- **Warehouse**: Dimensional model - dims & facts (tables)
- **Marts**: Business aggregations for reporting (tables)

## Project Structure
```
├── data/                   # Generated sample data
├── src/                    # Python scripts
│   ├── generators/         # Fake data generation
│   └── loaders/            # PostgreSQL data loader
├── ecommerce_dbt/          # dbt project
│   ├── models/
│   │   ├── staging/        # stg_customers, stg_orders, etc.
│   │   ├── warehouse/      # dim_customers, dim_products, dim_date, fact_sales
│   │   └── marts/          # mart_sales_daily, mart_customer_lifetime, mart_product_performance
│   └── snapshots/          # SCD Type 2 tracking
└── .github/workflows/      # CI/CD pipeline
```

## Data Model

### Dimensions
| Table | Description |
|-------|-------------|
| dim_customers | Customer attributes with tenure segmentation |
| dim_products | Product catalog with price tiers |
| dim_date | Calendar dimension (2023-2026) |

### Facts
| Table | Grain | Description |
|-------|-------|-------------|
| fact_sales | Order line item | Revenue, cost, profit metrics |

### Marts
| Table | Description |
|-------|-------------|
| mart_sales_daily | Daily aggregated revenue metrics |
| mart_customer_lifetime | RFM analysis and customer LTV |
| mart_product_performance | Product rankings and sales metrics |

## Setup

### Prerequisites
- Python 3.9+
- PostgreSQL 13+
- dbt-postgres

### Installation
```bash
# Clone repo
git clone https://github.com/YOUR_USERNAME/ecommerce-data-warehouse.git
cd ecommerce-data-warehouse

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create database
createdb ecommerce_dw

# Load sample data
python -m src.loaders.postgres_loader

# Run dbt
cd ecommerce_dbt
dbt build
```

## dbt Commands
```bash
# Run all models
dbt run

# Run tests
dbt test

# Build (run + test)
dbt build

# Generate docs
dbt docs generate
dbt docs serve

# Check source freshness
dbt source freshness

# Run snapshots
dbt snapshot

# Full refresh incremental models
dbt run --full-refresh --select fact_sales
```

## Testing

72 tests covering:
- Primary key uniqueness
- Not null constraints
- Referential integrity
- Accepted values validation

## CI/CD

GitHub Actions runs `dbt build` on every pull request to main.

## Tech Stack

- **Database**: PostgreSQL
- **Transformation**: dbt
- **Data Generation**: Python (Faker)
- **CI/CD**: GitHub Actions