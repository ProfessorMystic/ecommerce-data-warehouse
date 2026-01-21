-- Date dimension table
-- Pre-generated date attributes for time-based analysis

with date_spine as (
    select generate_series(
        '2023-01-01'::date,
        '2026-12-31'::date,
        '1 day'::interval
    )::date as date_day
),

dim_date as (
    select
        date_day as date_key,
        date_day,

        -- Date parts
        extract(year from date_day)::int as year,
        extract(quarter from date_day)::int as quarter,
        extract(month from date_day)::int as month,
        extract(week from date_day)::int as week_of_year,
        extract(day from date_day)::int as day_of_month,
        extract(dow from date_day)::int as day_of_week,
        extract(doy from date_day)::int as day_of_year,

        -- Date names
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_short,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_short,

        -- Formatted dates
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-Q') || 'Q' || extract(quarter from date_day) as year_quarter,

        -- Flags
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_day) between 1 and 5 then true else false end as is_weekday,

        -- Fiscal year (assuming starts in January)
        extract(year from date_day)::int as fiscal_year,
        extract(quarter from date_day)::int as fiscal_quarter

    from date_spine
)

select * from dim_date