CREATE TABLE IF NOT EXISTS raw_odoo_orders (
    order_id TEXT PRIMARY KEY,
    create_date TIMESTAMP,
    last_updated TIMESTAMP,        
    sales_date TIMESTAMP,
    date_order TIMESTAMP,
    delivery_date TIMESTAMP,
    delivery_address TEXT,
    sales_team TEXT,
    sales_person TEXT,
    amount_total NUMERIC,
    payment_terms TEXT,
    state TEXT,
    invoice_status TEXT,
    customer TEXT,
    shipping_policy TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);