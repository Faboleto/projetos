-------------- IMPORTS -----------------------
with 

ads as (
  select * from {{ source('sgt_facebook_ads', 'ads') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
  select 
    id ad_id,
    adset_id,
    campaign_id,
    name,
    account_id,
    received_at,
    status,
    url_parameters,
    utm_medium,
    uuid_ts,
    bid_type,
    received_date
  from ads),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final