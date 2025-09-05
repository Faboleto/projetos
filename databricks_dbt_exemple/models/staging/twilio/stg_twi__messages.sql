-------------- IMPORTS -----------------------
with

messages as (
  select * from {{ source('twilio', 'messages') }}),

users as (
  select * from {{ ref('stg_db__users') }}),

--------------- CUSTOM LOGIC -------------------------
final as (
  select 
    convert_timezone('America/Sao_Paulo', date_sent)::timestamp as date_sent,
    coalesce(ut.user_id, uf.user_id) as user_id,
    replace(to, 'whatsapp:', '') as send_to,
    replace(from, 'whatsapp:', '') as from,
    body as message,
    coalesce(price,0) as price,
    m.status,
    replace(direction, '-api', '') as direction
  from messages m
    left join users ut on (ut.tel = replace(to, 'whatsapp:', ''))
    left join users uf on (uf.tel = replace(from, 'whatsapp:', ''))
  order by date_sent desc)

--------------- FINAL CTE -------------------------
select * from final