-------------- IMPORTS -----------------------
with 

user_identifiers as (
  select * from {{ source('sgt_unify', 'user_identifiers') }}),

user_identifiers_old_schema as (
  select * from {{ source('sgt_old_schema', 'user_identifiers') }}),

--------------- CUSTOM LOGIC -------------------------

user_identifiers_actual as (
  select 
    id as user_identifier_id,
    value,
    `__profile_version` as profile_version,
    canonical_segment_id,
    received_at,
    seq,
    type,
    uuid_ts,
    received_date
  from user_identifiers),

user_identifiers_old as (
  select 
    id as user_identifier_id,
    value,
    `__profile_version` as profile_version,
    canonical_segment_id,
    received_at,
    seq,
    type,
    uuid_ts,
    null as received_date
  from user_identifiers_old_schema),

  user_identifiers_unified as (
    select * from user_identifiers_old
    union all
    select * from user_identifiers_actual),

  user_identifiers_unified_ordered as (
    select 
    *,
    row_number() over(partition by user_identifier_id order by received_at desc) as rn
    from user_identifiers_unified),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from user_identifiers_unified_ordered
  where rn = 1)
select * from final