-- dbt/models/intermediate/dim_shopify_sub_channels.sql
with distinct_subchannels as (
    select distinct sub_channel_name
    from {{ ref('stg_shopify_orders') }}
    where sub_channel_name is not null
)
select
    row_number() over (order by sub_channel_name) as sub_channel_id,
    sub_channel_name
from distinct_subchannels;
