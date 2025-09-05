-------------- IMPORTS -----------------------
with 

user_logged_in as (
  select * from {{ source('sgt_unify', 'user_logged_in') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
      -- Segment Default Fields
      id,
      event,
      event_text,
      event_source_name,
      context_library_version,
      event_source_slug,
      original_timestamp,
      received_at,
      uuid_ts,
      convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
      received_date,
      context_library_name,
      event_source_id,
      segment_id,
      -- Identity
      user_id,
      -- Event Properties
      email,
      name,
      first_login,
      platform
      coupon
    from user_logged_in),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final