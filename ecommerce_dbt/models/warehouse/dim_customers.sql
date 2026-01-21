-- Customer dimension table
-- Type 1 SCD: Overwrites with current values

with staged as (
    select * from {{ ref('stg_customers') }}
),

dim_customers as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        full_name,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        segment,
        registration_date,
        is_active,

        -- Derived attributes
        current_date - registration_date as days_since_registration,
        case
            when current_date - registration_date < 90 then 'New'
            when current_date - registration_date < 365 then 'Active'
            when current_date - registration_date < 730 then 'Mature'
            else 'Veteran'
        end as tenure_segment,

        created_at,
        updated_at
    from staged
)

select * from dim_customers