CREATE TABLE IF NOT EXISTS raw_shopify_orders (
    order_id TEXT PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    display_financial_status TEXT,
    original_total_price NUMERIC,
    current_discounts NUMERIC,
    current_subtotal NUMERIC,
    current_total_price NUMERIC,
    channel_name TEXT,
    sub_channel_name TEXT,
    total_tax NUMERIC,
    shipping_charges NUMERIC,
    shipping_charge_taxes NUMERIC,
    carrier_identifier TEXT,
    shipping_address1 TEXT,       -- New field: shipping address line 1
    shipping_city TEXT,           -- New field: shipping city
    shipping_zip TEXT,            -- New field: shipping zip code
    shipping_country_code TEXT,   -- New field: shipping country code
    discount_codes TEXT,          -- New field: comma-separated discount codes
    discount_total_amount NUMERIC,-- New field: total discount amount
    ingested_at TIMESTAMP DEFAULT now()
);