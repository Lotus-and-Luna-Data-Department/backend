-- dbt/models/staging/stg_shopify_orders.sql
-- This model pulls all fields from the raw_shopify_orders table.
-- It renames and maps fields to align with the Odoo staging model for a later union.
with raw as (
    select
        order_id,
        created_at,
        updated_at,
        display_financial_status,
        original_total_price,
        current_discounts,
        current_subtotal,
        current_total_price,
        channel_name,
        sub_channel_name,
        total_tax,
        shipping_charges,
        shipping_charge_taxes,
        carrier_identifier
    from {{ source('raw', 'raw_shopify_orders') }}
)

select
    order_id,
    -- Rename created_at to order_date to align with Odoo's naming
    created_at as order_date,
    updated_at,
    -- Use current_total_price as the key total amount
    current_total_price as total,
    total_tax,
    shipping_charges,
    shipping_charge_taxes,
    -- Preserve the channel and sub-channel names for dimension table creation later
    channel_name,
    sub_channel_name,
    carrier_identifier,
    -- Rename display_financial_status to order_status so both systems match
    display_financial_status as order_status,
    -- Also pass through other Shopify-specific numeric fields (if needed)
    original_total_price,
    current_subtotal,
    current_discounts,
    'shopify' as source
from raw;
