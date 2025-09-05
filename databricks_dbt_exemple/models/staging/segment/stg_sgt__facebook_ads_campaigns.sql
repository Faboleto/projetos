-------------- IMPORTS -----------------------
with 

campaigns as (
  select * from {{ source('sgt_facebook_ads', 'campaigns') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
  select 
    id as campaign_id,
    name,
    effective_status,
    account_id,
    received_at,
    start_time,
    uuid_ts,
    buying_type,
    received_date
  from campaigns),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final