
-------------- IMPORTS -----------------------
with 

accounts as (
  select * from {{ source('db', 'lnd_db_v2__accounts') }}),

--------------- CUSTOM LOGIC -------------------------
last_records as (
  select 
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at desc) as rn,
    * 
  from accounts),

transform as (
  select 
    id as account_id,
    convert_timezone('America/Sao_Paulo', created_at)::timestamp as created_at,
    convert_timezone('America/Sao_Paulo', updated_at)::timestamp as updated_at,
    name,
    type,
    user_id
  from last_records 
  where rn = 1),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final