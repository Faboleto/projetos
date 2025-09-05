-------------- IMPORTS -----------------------
with 

user_traits as (
  select * from {{ source('sgt_unify', 'user_traits') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
    select 
        id as user_trait_id,
        name,
        received_at,
        seq,
        uuid_ts,
        value,
        __profile_version,
        canonical_segment_id,
        received_date
    from user_traits),
--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final