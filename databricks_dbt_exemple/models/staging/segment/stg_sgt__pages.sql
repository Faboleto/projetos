-------------- IMPORTS -----------------------
with 

pages as (
  select * from {{ source('sgt_unify', 'pages') }}),

pages_old_schema as (
  select * from {{ source('sgt_old_schema', 'pages') }}),

--------------- CUSTOM LOGIC -------------------------

pages_actual as (
  select 
    id as page_id,
    event_source_id,
    received_at,
    context_campaign_source,
    context_library_name,
    context_page_path,
    context_page_referrer,
    context_user_agent,
    url,
    context_campaign_name,
    context_library_version,
    context_page_title,
    context_user_agent_data_platform,
    referrer,
    segment_id,
    context_page_search,
    convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
    user_id,
    context_user_agent_data_brands,
    original_timestamp,
    context_timezone,
    path,
    title,
    anonymous_id,
    context_campaign_content,
    event_source_slug,
    uuid_ts,
    sent_at,
    context_campaign_medium,
    context_ip,
    context_locale,
    context_page_url,
    context_user_agent_data_mobile,
    event_source_name,
    search,
    received_date,
    context_campaign_id,
    context_campaign_term
  from pages),

pages_old as (
  select 
    id as page_id,
    event_source_id,
    received_at,
    context_campaign_source,
    context_library_name,
    context_page_path,
    context_page_referrer,
    context_user_agent,
    url,
    context_campaign_name,
    context_library_version,
    context_page_title,
    context_user_agent_data_platform,
    referrer,
    segment_id,
    context_page_search,
    convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
    user_id,
    context_user_agent_data_brands,
    original_timestamp,
    context_timezone,
    path,
    title,
    anonymous_id,
    context_campaign_content,
    event_source_slug,
    uuid_ts,
    sent_at,
    context_campaign_medium,
    context_ip,
    context_locale,
    context_page_url,
    context_user_agent_data_mobile,
    event_source_name,
    search,
    null received_date,
    context_campaign_id,
    context_campaign_term
  from pages_old_schema),

  pages_unified as (
    select * from pages_old
    union all
    select * from pages_actual),

  pages_unified_ordered as (
    select 
    *,
    row_number() over(partition by page_id order by `timestamp` desc) as rn
    from pages_unified),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from pages_unified_ordered
)
select * from final