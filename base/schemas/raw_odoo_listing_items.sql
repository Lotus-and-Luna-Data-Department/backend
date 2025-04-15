CREATE TABLE IF NOT EXISTS raw_odoo_listing_items (
    item_id TEXT PRIMARY KEY,
    continue_selling BOOLEAN,
    default_code TEXT,
    is_listed BOOLEAN,
    listing TEXT,
    marketplace_id TEXT,
    sale_price NUMERIC,
    created_on TIMESTAMP,
    last_updated TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now()
);
