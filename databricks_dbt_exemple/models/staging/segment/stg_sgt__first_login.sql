-------------- IMPORTS -----------------------
with 

first_login as (
  select * from {{ source('sgt_unify', 'first_login') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
        id as first_login_id,
        name,
        original_timestamp,
        platform,
        sent_at,
        email,
        event_source_id,
        event_source_slug,
        received_at,
        segment_id,
        context_library_name,
        context_library_version,
        event,
        uuid_ts,
        `timestamp`,
        user_id,
        event_source_name,
        event_text,
        first_login,
        received_date,
        referrer_url,
        source,
        context_page_path,
        context_page_title,
        context_ip,
        context_user_agent_data_platform,
        context_user_agent_data_mobile,
        medium,
        context_user_agent,
        context_user_agent_data_brands,
        context_locale,
        gaid,
        anonymous_id,
        context_page_url,
        context_timezone
    from first_login),
    
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final