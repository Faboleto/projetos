-------------- IMPORTS -----------------------
with 

campaign_performance_reports as (
  select * from {{ source('sgt_google_ads', 'campaign_performance_reports') }}),

campaign_performance_reports_legacy as (
  select * from {{ source('sgt_old_schema', 'google_ads__campaign_performance_reports') }}),

--------------- CUSTOM LOGIC -------------------------

transform_original as (
  select 
    id as campaign_performance_report_id,
    impressions,
    clicks,
    cost,
    invalid_clicks,
    engagements,
    conversions,
    video_views,
    interactions,
    active_view_measurability,
    average_cost,
    campaign_id::string,
    campaign_status,
    campaign_trial_type,
    date_stop,
    gmail_saves,
    adwords_customer_id,
    all_conversion_rate,
    date_start,
    active_view_impressions,
    uuid_ts,
    view_through_conversions,
    base_campaign_id,
    received_at,
    video_quartile_25_rate
    video_quartile_50_rate,
    video_quartile_75_rate,
    video_quartile_100_rate,
    conversion_value,
    is_budget_explicitly_shared,
    value_per_all_conversion,
    active_view_measurable_impressions,
    all_conversion_value,
    amount,
    bounce_rate,
    budget_id,
    interaction_types,
    average_time_on_site,
    gmail_secondary_clicks,
    active_view_measurable_cost, 
    active_view_viewability,
    advertising_channel_sub_type,
    all_conversions,
    gmail_forwards,
    video_view_rate,
    received_date
  from campaign_performance_reports),

  transform_legacy as (
  select 
    id as campaign_performance_report_id,
    impressions,
    clicks,
    cost,
    invalid_clicks,
    engagements,
    conversions,
    video_views,
    interactions,
    active_view_measurability,
    average_cost,
    campaign_id,
    campaign_status,
    campaign_trial_type,
    date_stop,
    gmail_saves,
    adwords_customer_id,
    all_conversion_rate,
    date_start,
    active_view_impressions,
    uuid_ts,
    view_through_conversions,
    base_campaign_id,
    received_at,
    video_quartile_25_rate
    video_quartile_50_rate,
    video_quartile_75_rate,
    video_quartile_100_rate,
    conversion_value,
    is_budget_explicitly_shared,
    value_per_all_conversion,
    active_view_measurable_impressions,
    all_conversion_value,
    amount,
    bounce_rate,
    budget_id,
    interaction_types,
    average_time_on_site,
    gmail_secondary_clicks,
    active_view_measurable_cost, 
    active_view_viewability,
    advertising_channel_sub_type,
    all_conversions,
    gmail_forwards,
    video_view_rate,
    null received_date
  from campaign_performance_reports_legacy),


  unified as (
    select * from transform_original
    union all
    select * from transform_legacy where campaign_performance_report_id not in (select distinct campaign_performance_report_id from transform_original)),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from unified)
select * from final