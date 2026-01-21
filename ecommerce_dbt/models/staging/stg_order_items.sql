-- Staging model for order items
-- Cleans and standardizes order line item data from source

with source as (
    select * from {{ source('staging', 'order_items') }}
),

staged as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount,
        line_total,
        unit_price - discount as discounted_price,
        round(discount / nullif(unit_price, 0) * 100, 2) as discount_percent
    from source
)

select * from staged