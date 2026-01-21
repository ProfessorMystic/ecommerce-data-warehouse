-- ============================================================================
-- CUSTOMER LIFETIME VALUE MART
-- ============================================================================
-- RFM analysis (Recency, Frequency, Monetary) and lifetime metrics.
-- Used for customer segmentation and retention analysis.
-- ============================================================================

with customers as (
    select * from {{ ref('dim_customers') }}
),

sales as (
    select * from {{ ref('fact_sales') }}
    where order_status not in ('cancelled', 'returned')
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_units_purchased,
        sum(gross_amount) as lifetime_revenue,
        sum(profit_amount) as lifetime_profit,
        sum(discount) as lifetime_discounts
    from sales
    group by 1
),

customer_metrics as (
    select
        c.customer_id,
        c.email,
        c.full_name,
        c.segment,
        c.tenure_segment,
        c.registration_date,
        c.is_active,
        c.city,
        c.state,

        -- Order metrics
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.total_units_purchased, 0) as total_units_purchased,

        -- Revenue metrics
        round(coalesce(co.lifetime_revenue, 0)::numeric, 2) as lifetime_revenue,
        round(coalesce(co.lifetime_profit, 0)::numeric, 2) as lifetime_profit,
        round(coalesce(co.lifetime_discounts, 0)::numeric, 2) as lifetime_discounts,

        -- Averages
        round(coalesce(co.lifetime_revenue, 0)::numeric / nullif(co.total_orders, 0), 2) as avg_order_value,

        -- Dates
        co.first_order_date,
        co.last_order_date,

        -- Recency (days since last order)
        current_date - co.last_order_date::date as days_since_last_order,

        -- Purchase frequency segment
        case
            when co.total_orders is null then 'Never Purchased'
            when co.total_orders = 1 then 'One-Time'
            when co.total_orders between 2 and 3 then 'Repeat'
            when co.total_orders between 4 and 10 then 'Loyal'
            else 'VIP'
        end as purchase_frequency_segment,

        -- Recency segment
        case
            when co.last_order_date is null then 'Never Purchased'
            when current_date - co.last_order_date::date <= 30 then 'Active'
            when current_date - co.last_order_date::date <= 90 then 'Recent'
            when current_date - co.last_order_date::date <= 180 then 'Lapsed'
            else 'Dormant'
        end as recency_segment,

        -- Value segment
        case
            when coalesce(co.lifetime_revenue, 0) = 0 then 'No Value'
            when co.lifetime_revenue < 100 then 'Low Value'
            when co.lifetime_revenue < 500 then 'Medium Value'
            when co.lifetime_revenue < 2000 then 'High Value'
            else 'Premium'
        end as value_segment

    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from customer_metrics