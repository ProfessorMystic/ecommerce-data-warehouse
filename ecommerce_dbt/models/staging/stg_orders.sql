-- Staging model for orders
-- Cleans and standardizes order data from source

with source as (
    select * from {{ source('staging', 'orders') }}
),

staged as (
    select
        order_id,
        customer_id,
        order_date,
        date(order_date) as order_date_day,
        status,
        subtotal,
        shipping,
        tax,
        total,
        payment_method,
        shipping_address,
        shipping_city,
        shipping_state,
        shipping_zip,
        created_at,
        updated_at
    from source
)

select * from staged