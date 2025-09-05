-------------- IMPORTS -----------------------
with 

mobile_attribution as (
  select * from {{ source('sgt_unify', 'mobile_attribution') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
        id as mobile_attribution_id,
        context_page_path,
        medium,
        source,
        anonymous_id,
        user_id,
        context_user_agent_data_mobile,
        context_page_title,
        context_page_url,
        event_source_id,
        referrer_url,
        segment_id,
        `timestamp`,
        uuid_ts,
        context_library_name,
        context_library_version,
        email,
        event, 
        context_ip,
        context_timezone,
        context_locale,
        event_text,
        context_user_agent,
        context_user_agent_data_platform,
        context_user_agent_data_brands,
        event_source_slug,
        received_at,
        sent_at,
        gaid,
        original_timestamp,
        event_source_name,
        received_date
    from mobile_attribution),
    
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final