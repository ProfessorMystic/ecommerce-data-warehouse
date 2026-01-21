-- ============================================================================
-- CUSTOMER SNAPSHOT (SCD TYPE 2)
-- ============================================================================
-- Tracks historical changes to customer records.
-- Creates new row when segment, is_active, or address changes.
-- ============================================================================

{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['segment', 'is_active', 'address', 'city', 'state', 'zip_code']
    )
}}

select * from {{ source('staging', 'customers') }}

{% endsnapshot %}