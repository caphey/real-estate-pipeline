select
    symbol,
    "date" as trade_date,
    "open" as opening_price,
    high as high_price,
    low as low_price,
    "close" as closing_price,
    volume

from {{ source('public', 'raw_stock_data') }}