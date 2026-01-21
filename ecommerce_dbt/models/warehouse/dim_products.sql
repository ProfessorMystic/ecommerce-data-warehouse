-- Product dimension table
-- Includes category denormalization for easier querying

with staged as (
    select * from {{ ref('stg_products') }}
),

dim_products as (
    select
        product_id,
        sku,
        product_name,
        description,
        category_id,
        category_name,
        price,
        cost,
        profit_margin,
        margin_percent,
        stock_quantity,
        is_active,

        -- Price tier classification
        case
            when price < 25 then 'Budget'
            when price < 100 then 'Mid-Range'
            when price < 500 then 'Premium'
            else 'Luxury'
        end as price_tier,

        -- Stock status
        case
            when stock_quantity = 0 then 'Out of Stock'
            when stock_quantity < 20 then 'Low Stock'
            when stock_quantity < 100 then 'Normal'
            else 'Well Stocked'
        end as stock_status,

        created_at,
        updated_at
    from staged
)

select * from dim_products