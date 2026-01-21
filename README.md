# E-Commerce Data Warehouse

Production-ready data warehouse built with dbt and PostgreSQL, implementing dimensional modeling for e-commerce analytics.

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
├── data/generated/         # Sample CSV data
├── src/
│   ├── generators/         # Fake data generation
│   └── loaders/            # PostgreSQL loader
├── ecommerce_dbt/
│   ├── models/
│   │   ├── staging/        # stg_customers, stg_orders, stg_products, stg_order_items
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
| fact_sales | Order line item | Revenue, cost, profit metrics (incremental) |

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
git clone https://github.com/ProfessorMystic/ecommerce-data-warehouse.git
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
dbt run              # Run all models
dbt test             # Run tests
dbt build            # Run + test
dbt docs generate    # Generate docs
dbt docs serve       # View docs locally
dbt source freshness # Check source freshness
dbt snapshot         # Run SCD snapshots
```

## Testing

72 data tests covering:
- Primary key uniqueness
- Not null constraints  
- Referential integrity
- Accepted values validation

## Tech Stack

- **Database**: PostgreSQL
- **Transformation**: dbt
- **Data Generation**: Python (Faker)
- **CI/CD**: GitHub Actions
