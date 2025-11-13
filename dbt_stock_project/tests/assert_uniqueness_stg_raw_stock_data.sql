SELECT
    trade_date,
    symbol,
    COUNT(*) AS count_of_records
FROM
    {{ ref('stg_raw_stock_data') }}
GROUP BY
    trade_date,
    symbol
HAVING
    COUNT(*) > 1