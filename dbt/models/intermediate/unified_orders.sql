-- dbt/models/intermediate/unified_orders.sql

with odoo as (
    select
        order_id,
        order_date,
        updated_at,
        total,
        order_status,
        customer,
        delivery_address,
        shipping_policy,
        sales_team,
        /* Odoo has no channel or sub-channel info so leave them as NULL */
        null as channel_id,
        null as sub_channel_id,
        null as channel_name,
        null as sub_channel_name,
        null as shipping_charges,
        null as shipping_charge_taxes,
        null as carrier_identifier,
        'odoo' as source
    from {{ ref('stg_odoo_orders') }}
),

shopify_base as (
    select
        order_id,
        order_date,
        updated_at,
        total,
        order_status,
        channel_name,
        sub_channel_name,
        shipping_charges,
        shipping_charge_taxes,
        carrier_identifier,
        'shopify' as source
    from {{ ref('stg_shopify_orders') }}
),

shopify as (
    select
        sb.order_id,
        sb.order_date,
        sb.updated_at,
        sb.total,
        sb.order_status,
        /* Shopify does not have these fields, so leave them as NULL */
        null as customer,
        null as delivery_address,
        null as shipping_policy,
        null as sales_team,
        /* Join to get the surrogate keys and preserved text values */
        dch.channel_id,
        dsch.sub_channel_id,
        dch.channel_name,
        dsch.sub_channel_name,
        sb.shipping_charges,
        sb.shipping_charge_taxes,
        sb.carrier_identifier,
        sb.source
    from shopify_base sb
    left join {{ ref('dim_shopify_channels') }} dch
      on sb.channel_name = dch.channel_name
    left join {{ ref('dim_shopify_sub_channels') }} dsch
      on sb.sub_channel_name = dsch.sub_channel_name
)

select * from odoo
union all
select * from shopify;
