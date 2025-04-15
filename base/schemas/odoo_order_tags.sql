CREATE TABLE IF NOT EXISTS odoo_order_tags (
    order_id TEXT REFERENCES raw_odoo_orders(order_id),
    tag TEXT NOT NULL,
    PRIMARY KEY (order_id, tag)
);