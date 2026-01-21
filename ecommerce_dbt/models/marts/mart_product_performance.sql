-- ============================================================================
-- PRODUCT PERFORMANCE MART
-- ============================================================================
-- Sales metrics by product and category for merchandising analysis.
-- Includes revenue rankings, profit analysis, and sales velocity.
-- ============================================================================

with products as (
    select * from {{ ref('dim_products') }}
),

sales as (
    select * from {{ ref('fact_sales') }}
    where order_status not in ('cancelled', 'returned')
),

product_sales as (
    select
        product_id,
        count(distinct order_id) as total_orders,
        count(*) as total_line_items,
        sum(quantity) as total_units_sold,
        sum(gross_amount) as total_revenue,
        sum(cost_amount) as total_cost,
        sum(profit_amount) as total_profit,
        sum(discount) as total_discounts,
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date
    from sales
    group by 1
),

product_metrics as (
    select
        p.product_id,
        p.sku,
        p.product_name,
        p.category_id,
        p.category_name,
        p.price,
        p.cost,
        p.price_tier,
        p.stock_quantity,
        p.stock_status,
        p.is_active,

        -- Sales metrics
        coalesce(ps.total_orders, 0) as total_orders,
        coalesce(ps.total_units_sold, 0) as total_units_sold,
        round(coalesce(ps.total_revenue, 0)::numeric, 2) as total_revenue,
        round(coalesce(ps.total_cost, 0)::numeric, 2) as total_cost,
        round(coalesce(ps.total_profit, 0)::numeric, 2) as total_profit,
        round(coalesce(ps.total_discounts, 0)::numeric, 2) as total_discounts,

        -- Calculated metrics
        round(coalesce(ps.total_revenue, 0)::numeric / nullif(ps.total_units_sold, 0), 2) as avg_selling_price,
        round(coalesce(ps.total_profit, 0)::numeric / nullif(ps.total_revenue, 0) * 100, 2) as profit_margin_pct,

        -- Dates
        ps.first_sale_date,
        ps.last_sale_date,

        -- Rankings (within category)
        rank() over (partition by p.category_name order by coalesce(ps.total_revenue, 0) desc) as revenue_rank_in_category,
        rank() over (partition by p.category_name order by coalesce(ps.total_units_sold, 0) desc) as units_rank_in_category,

        -- Overall rankings
        rank() over (order by coalesce(ps.total_revenue, 0) desc) as overall_revenue_rank,
        rank() over (order by coalesce(ps.total_units_sold, 0) desc) as overall_units_rank

    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from product_metrics