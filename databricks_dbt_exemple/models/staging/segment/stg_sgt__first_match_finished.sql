-------------- IMPORTS -----------------------
with 

first_match_finished as (
  select * from {{ source('sgt_unify', 'first_match_finished') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
        event,
        id as first_match_finished_id, 
        original_timestamp,
        segment_id,
        transaction_id,
        context_library_name,
        event_source_slug,
        event_text,
        convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
        uuid_ts,
        coupon_did_win,
        coupon_id,
        event_source_id,
        user_id,
        context_library_version,
        coupon_balance,
        event_source_name,
        gamemode,
        sent_at,
        received_at,
        coupon_max_amount,
        coupon_amount,
        coupon_amount_real,
        user_tip_score
    from first_match_finished),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final