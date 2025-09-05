-------------- IMPORTS -----------------------
with 

ads as (
  select * from {{ source('facebook_ads', 'ads') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
      id as ad_id,
      name,
      status,
      adlabels,
      adset_id,
      bid_info,
      bid_type,
      creative,
      targeting,
      account_id,
      bid_amount,
      campaign_id,
      created_time,
      updated_time,
      source_ad_id,
      tracking_specs,
      recommendations,
      conversion_specs,
      effective_status,
      last_updated_by_app_id
    from ads),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final