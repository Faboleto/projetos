with

list_prices as (
  select * from {{ source('databricks', 'list_prices') }}),

final as (
  select 
    account_id,
    price_start_time,
    price_end_time,
    sku_name,
    cloud,
    currency_code,
    usage_unit,
    pricing
  from list_prices)
select * from final