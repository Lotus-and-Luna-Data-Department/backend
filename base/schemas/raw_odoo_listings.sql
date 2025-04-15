CREATE TABLE IF NOT EXISTS raw_odoo_listings (
    listing_id TEXT PRIMARY KEY,
    name TEXT,
    display_name TEXT,
    product_template TEXT,
    marketplace_id TEXT,
    marketplace_instance TEXT,
    is_listed BOOLEAN,
    is_published BOOLEAN,
    allow_sales_when_out_of_stock BOOLEAN,
    created_on TIMESTAMP,
    last_updated TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now()
);

