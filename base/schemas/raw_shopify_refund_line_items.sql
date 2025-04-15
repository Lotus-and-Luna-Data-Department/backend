CREATE TABLE IF NOT EXISTS raw_shopify_refund_lines (
    refund_line_item_id TEXT PRIMARY KEY,
    refund_id TEXT,
    order_id TEXT,
    quantity NUMERIC,
    subtotal NUMERIC,
    tax_subtotal NUMERIC,
    line_item_id TEXT,
    title TEXT,
    sku TEXT,
    product_id TEXT,
    product_type TEXT,
    variant_id TEXT,
    variant_sku TEXT,
    ingested_at TIMESTAMP DEFAULT now()
);
