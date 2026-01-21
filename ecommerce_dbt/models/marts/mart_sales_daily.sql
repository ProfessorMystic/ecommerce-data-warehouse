-- ============================================================================
-- DAILY SALES MART
-- ============================================================================
-- Aggregates sales metrics by day for dashboards and reporting.
-- Includes revenue, order counts, AOV, and profit calculations.
-- ============================================================================

with sales as (
    select * from {{ ref('fact_sales') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

daily_metrics as (
    select
        s.date_key,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        d.year_month,
        d.year_quarter,

        -- Order metrics
        count(distinct s.order_id) as total_orders,
        count(distinct s.customer_id) as unique_customers,
        sum(s.quantity) as total_units_sold,

        -- Revenue metrics
        round(sum(s.gross_amount)::numeric, 2) as gross_revenue,
        round(sum(s.discount)::numeric, 2) as total_discounts,
        round(sum(s.cost_amount)::numeric, 2) as total_cost,
        round(sum(s.profit_amount)::numeric, 2) as gross_profit,

        -- Calculated metrics
        round(sum(s.gross_amount)::numeric / nullif(count(distinct s.order_id), 0), 2) as avg_order_value,
        round(sum(s.profit_amount)::numeric / nullif(sum(s.gross_amount), 0) * 100, 2) as profit_margin_pct,

        -- Order status breakdown
        count(distinct case when s.order_status = 'completed' then s.order_id end) as completed_orders,
        count(distinct case when s.order_status = 'cancelled' then s.order_id end) as cancelled_orders,
        count(distinct case when s.order_status = 'returned' then s.order_id end) as returned_orders

    from sales s
    left join dates d on s.date_key = d.date_key
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

select * from daily_metrics
order by date_key