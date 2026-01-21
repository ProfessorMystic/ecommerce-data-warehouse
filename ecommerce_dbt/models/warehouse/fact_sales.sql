-- ============================================================================
-- SALES FACT TABLE (INCREMENTAL)
-- ============================================================================
-- Grain: One row per order line item.
-- Incremental strategy: Append new records based on order_date.
-- Full refresh: dbt run --full-refresh --select fact_sales
-- ============================================================================

{{
    config(
        materialized='incremental',
        unique_key='sales_key',
        incremental_strategy='delete+insert'
    )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

fact_sales as (
    select
        -- Keys
        oi.order_item_id as sales_key,
        o.order_id,
        o.customer_id,
        oi.product_id,
        o.order_date_day as date_key,

        -- Order attributes
        o.status as order_status,
        o.payment_method,

        -- Measures
        oi.quantity,
        oi.unit_price,
        oi.discount,
        oi.discounted_price,
        oi.line_total as gross_amount,

        -- Calculated measures
        p.cost * oi.quantity as cost_amount,
        oi.line_total - (p.cost * oi.quantity) as profit_amount,

        -- Timestamps
        o.order_date,
        o.created_at

    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    inner join products p on oi.product_id = p.product_id

    {% if is_incremental() %}
        -- Only process records newer than the latest in the table
        where o.order_date > (select max(order_date) from {{ this }})
    {% endif %}
)

select * from fact_sales