-------------- IMPORTS -----------------------
with 

ads_insights as (
  select * from {{ source('facebook_ads', 'ads_insights') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
      adset_id,
      ad_id,
      account_id,
      campaign_id,
      adset_name,
      ad_name,
      objective,
      campaign_name,
      date_start,
      date_stop,
      created_time,
      updated_time,
      cpc::float,
      cpm::float,
      cpp::float,
      ctr::float,
      reach,
      spend::float,
      clicks,
      impressions,
      unique_clicks,
      conversions,
      actions
    from ads_insights),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final