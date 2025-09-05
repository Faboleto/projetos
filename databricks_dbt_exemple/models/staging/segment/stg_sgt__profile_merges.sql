-------------- IMPORTS -----------------------
with 

profile_merges as (
  select * from {{ source('sgt_unify', 'profile_merges') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
      id as profile_merge_id,
      received_at,
      segment_id,
      seq,
      uuid_ts,
      `__profile_version`,
      canonical_segment_id,
      received_date
    from profile_merges),
    
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final