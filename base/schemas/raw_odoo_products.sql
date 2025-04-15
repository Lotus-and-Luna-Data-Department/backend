CREATE TABLE IF NOT EXISTS raw_odoo_products (
    product_id TEXT PRIMARY KEY,
    internal_reference TEXT,
    name TEXT,
    qty_on_hand NUMERIC,
    free_qty NUMERIC,
    outgoing_qty NUMERIC,
    incoming_qty NUMERIC,
    last_updated TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now()
);