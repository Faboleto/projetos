-------------- IMPORTS -----------------------
with 

id_graph_updates as (
  select * from {{ source('sgt_unify', 'id_graph_updates') }}),

id_graph_updates_old_schema as (
  select * from {{ source('sgt_old_schema', 'id_graph_updates') }}),

--------------- CUSTOM LOGIC -------------------------

id_graph_updates_actual as (
  select 
    id as id_graph_update_id,
    canonical_segment_id,
    triggering_event_id,
    triggering_event_source_name,
    triggering_event_source_slug,
    uuid_ts,
    triggering_event_type, 
    received_at,
    segment_id,
    seq,
    convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
    triggering_event_name,
    triggering_event_source_id,
    received_date
  from id_graph_updates),

id_graph_updates_old as (
  select 
    id as id_graph_update_id,
    canonical_segment_id,
    triggering_event_id,
    triggering_event_source_name,
    triggering_event_source_slug,
    uuid_ts,
    triggering_event_type, 
    received_at,
    segment_id,
    seq,
    convert_timezone('America/Sao_Paulo', `timestamp`)::timestamp as `timestamp`,
    triggering_event_name,
    triggering_event_source_id,
    null as received_date
  from id_graph_updates_old_schema),

  id_graph_updates_unified as (
    select * from id_graph_updates_actual
    union all
    select * from id_graph_updates_old),

  id_graph_updates_unified_ordered as (
    select 
    *,
    row_number() over(partition by id_graph_update_id order by `timestamp` desc) as rn
    from id_graph_updates_unified),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from id_graph_updates_unified_ordered
  where rn = 1)
select * from final