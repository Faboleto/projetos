-------------- IMPORTS -----------------------
with 

coupon_created as (
  select * from {{ source('sgt_unify', 'coupon_created') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
      id,
      event_source_slug,
      original_timestamp,
      received_at,
      segment_id,
      user_id,
      context_library_version,
      coupon_amount_real,
      event_source_name,
      event_text,
      gamemode,
      convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
      `_id`,
      coupon_amount,
      event,
      fraud_score,
      is_first_coupon,
      is_first_coupon_at_gamemode,
      uuid_ts,
      context_library_name,
      coupon_id,
      coupon_max_amount,
      event_source_id,
      received_date,
      amount,
      external_gamemode
    from coupon_created),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final