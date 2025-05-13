CREATE TABLE IF NOT EXISTS raw_odoo_account_moves (
    move_id TEXT PRIMARY KEY,
    sale_order_refs TEXT,
    move_type TEXT,
    state TEXT,
    amount_residual NUMERIC,
    amount_total NUMERIC,
    invoice_date_due TIMESTAMP,
    create_date TIMESTAMP,
    last_updated TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);
