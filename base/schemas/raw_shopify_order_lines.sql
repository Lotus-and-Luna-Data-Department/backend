CREATE TABLE IF NOT EXISTS raw_shopify_order_lines (
    line_item_id TEXT PRIMARY KEY,
    order_id TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    title TEXT,
    quantity NUMERIC,
    sku TEXT,
    product_id TEXT,
    product_type TEXT,
    variant_id TEXT,
    variant_sku TEXT,
    variant_price NUMERIC,
    original_total NUMERIC,
    discounted_total NUMERIC,
    discount_codes TEXT,          -- New field: comma-separated discount codes
    discount_amounts TEXT,        -- New field: comma-separated discount amounts
    ingested_at TIMESTAMP DEFAULT now()
);