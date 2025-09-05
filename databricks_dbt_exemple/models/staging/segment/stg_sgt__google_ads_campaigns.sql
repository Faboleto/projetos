-------------- IMPORTS -----------------------
with 

campaigns as (
  select * from {{ source('sgt_google_ads', 'campaigns') }}),

campaigns_legacy as (
  select * from {{ source('sgt_old_schema', 'google_ads__campaigns') }}),

--------------- CUSTOM LOGIC -------------------------

transform_original as (
  select 
    id::string as campaign_id,
    adwords_customer_id,
    serving_status,
    status,
    name,
    start_date,
    end_date,
    received_at,
    uuid_ts,
    received_date
  from campaigns),

transform_legacy as (
  select 
    id::string as campaign_id,
    adwords_customer_id,
    serving_status,
    status,
    name,
    start_date,
    end_date,
    received_at,
    uuid_ts,
    null received_date
  from campaigns_legacy),

unified as (
  select * from transform_original 
  union all
  select * from transform_legacy where campaign_id not in (select distinct campaign_id from transform_original)),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from unified)
select * from final