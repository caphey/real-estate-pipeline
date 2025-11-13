select * from {{ ref('stg_raw_stock_data') }}
where closing_price < 0