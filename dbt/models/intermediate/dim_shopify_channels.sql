-- dbt/models/intermediate/dim_shopify_channels.sql
with distinct_channels as (
    select distinct channel_name, sub_channel_name
    from {{ ref('stg_shopify_orders') }}
    where (channel_name is not null or sub_channel_name is not null)
)
select
    row_number() over (order by coalesce(channel_name, ''), coalesce(sub_channel_name, '')) as channel_id,
    channel_name,
    sub_channel_name
from distinct_channels;
