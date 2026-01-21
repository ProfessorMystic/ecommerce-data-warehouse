-- Staging model for customers
-- Cleans and standardizes customer data from source

with source as (
    select * from {{ source('staging', 'customers') }}
),

staged as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        segment,
        registration_date,
        is_active,
        created_at,
        updated_at
    from source
)

select * from staged