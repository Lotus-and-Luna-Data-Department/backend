CREATE TABLE IF NOT EXISTS raw_shopify_refunds (
    refund_id TEXT PRIMARY KEY,
    order_id TEXT,
    created_at TIMESTAMP,
    processed_at TIMESTAMP,
    updated_at TIMESTAMP,
    total_refunded NUMERIC,
    refund_amount NUMERIC,
    sale_id TEXT,
    product_title TEXT,
    gross_returns NUMERIC,
    discounts_returned NUMERIC,
    shipping_returned NUMERIC,
    taxes_returned NUMERIC,
    return_fees NUMERIC,
    net_returns NUMERIC,
    total_returns NUMERIC,
    ingested_at TIMESTAMP DEFAULT now()
);
