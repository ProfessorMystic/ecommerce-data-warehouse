-- ============================================================================
-- PRODUCT SNAPSHOT (SCD TYPE 2)
-- ============================================================================
-- Tracks historical changes to product records.
-- Creates new row when price, cost, or stock changes.
-- ============================================================================

{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['price', 'cost', 'stock_quantity', 'is_active']
    )
}}

select * from {{ source('staging', 'products') }}

{% endsnapshot %}