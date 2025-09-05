with

usage as (
  select * from {{ source('databricks', 'usage') }}),

final as (
  select 
    account_id,
    workspace_id,
    record_id,
    sku_name,
    cloud,
    usage_start_time,
    usage_end_time,
    usage_date,
    custom_tags,
    usage_unit,
    usage_quantity,
    usage_metadata,
    identity_metadata,
    record_type,
    ingestion_date,
    billing_origin_product,
    product_features,
    usage_type
  from usage)
select * from final