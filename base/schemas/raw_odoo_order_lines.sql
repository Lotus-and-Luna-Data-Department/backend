CREATE TABLE IF NOT EXISTS raw_odoo_order_lines (
    line_item_id TEXT PRIMARY KEY,
    order_id TEXT,
    sales_date TIMESTAMP,
    last_updated TIMESTAMP,
    sku TEXT,
    quantity NUMERIC,
    unit_price NUMERIC,
    product_category TEXT,
    to_invoice TEXT,
    cost NUMERIC,
    sales_team TEXT,
    ingested_at TIMESTAMP DEFAULT now()
);
