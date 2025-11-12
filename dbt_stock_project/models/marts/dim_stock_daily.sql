with staging_data as (
    select * from {{ ref('stg_raw_stock_data') }}
),

calculated as (
    select
        *,
        lag(closing_price) over (partition by symbol order by trade_date) as previous_day_closing_price
    from staging_data
)

select
    symbol,
    trade_date,
    opening_price,
    high_price,
    low_price,
    closing_price,
    previous_day_closing_price,
    round(
        CASE 
            WHEN previous_day_closing_price = 0 THEN 0
            ELSE (closing_price - previous_day_closing_price) / previous_day_closing_price * 100
        END, 2
    ) as percent_change_from_previous_day

from calculated
order by symbol, trade_date desc
