-------------- IMPORTS -----------------------
with 

user_attribution_gtm as (
  select * from {{ source('sgt_unify', 'user_attribution_gtm') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
        id,
        event_source_name,
        traits_phone,
        context_user_agent_data_mobile,
        event,
        context_user_agent_data_brands,
        context_user_agent_data_platform,
        context_page_title,
        traits_gclid,
        event_source_id,
        received_at,
        segment_id,
        traits_ip,
        anonymous_id,
        context_library_name,
        original_timestamp,
        convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
        context_ip,
        event_source_slug,
        traits_email,
        traits_gbraid,
        traits_ttclid,
        traits_user_agent,
        uuid_ts,
        event_text,
        sent_at,
        context_page_referrer,
        context_timezone,
        context_user_agent,
        traits_fbp,
        traits_user_id,
        context_locale,
        context_page_path,
        traits_wbraid,
        user_id,
        context_library_version,
        context_page_url,
        received_date,
        context_campaign_content,
        context_campaign_term,
        context_page_search,
        context_campaign_medium,
        context_campaign_source,
        context_campaign_name,
        context_campaign_id,
        traits_fbclid
    from user_attribution_gtm),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final