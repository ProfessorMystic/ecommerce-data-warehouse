-- Staging model for products
-- Cleans and standardizes product data from source

with source as (
    select * from {{ source('staging', 'products') }}
),

staged as (
    select
        product_id,
        sku,
        name as product_name,
        description,
        category_id,
        category_name,
        price,
        cost,
        price - cost as profit_margin,
        round((price - cost) / nullif(price, 0) * 100, 2) as margin_percent,
        stock_quantity,
        is_active,
        created_at,
        updated_at
    from source
)

select * from staged