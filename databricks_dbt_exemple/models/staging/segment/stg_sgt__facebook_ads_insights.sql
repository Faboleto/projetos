-------------- IMPORTS -----------------------
with 

insights as (
  select * from {{ source('sgt_facebook_ads', 'insights') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
  select 
    id as insight_id,
    ad_id,
    reach,
    clicks,
    impressions,
    link_clicks,
    unique_impressions,
    unique_clicks,
    spend,
    social_spend,
    inline_post_engagements,
    frequency,
    uuid_ts,
    date_start,
    date_stop,
    received_at
  from insights),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final