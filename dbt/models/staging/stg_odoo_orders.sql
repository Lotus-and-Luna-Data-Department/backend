-- dbt/models/staging/stg_odoo_orders.sql
-- This model pulls all fields from the raw_odoo_orders table and aligns column names with Shopify staging.
with raw as (
    select
        order_id,
        create_date,
        write_date as updated_at,     -- Rename write_date for consistency with Shopify’s updated_at
        sales_date,
        date_order,
        delivery_date,
        delivery_address,
        sales_team,
        sales_person,
        amount_total,
        payment_terms,
        tags,
        state,
        invoice_status,
        customer,
        shipping_policy
    from {{ source('raw', 'raw_odoo_orders') }}
)

select 
    order_id,
    -- Use date_order as the unified order_date
    date_order as order_date,
    updated_at,
    sales_team,
    -- Use amount_total as the unified total
    amount_total as total,
    payment_terms,
    tags,
    -- Rename state to order_status to align with Shopify
    state as order_status,
    invoice_status,
    customer,
    delivery_address,
    sales_person,
    shipping_policy,
    'odoo' as source
from raw;
