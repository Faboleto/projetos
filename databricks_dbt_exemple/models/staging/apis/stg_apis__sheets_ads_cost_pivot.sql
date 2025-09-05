-------------- IMPORTS -----------------------
with 

ads_cost as (
  select * from {{ source('apis', 'lnd_sheets__ads_cost_pivot') }}),

--------------- CUSTOM LOGIC -------------------------

transform as (
  select 
    cast(reference_date as date) as reference_date,
    cost,
    font
  from ads_cost
  where reference_date::date < now()::date),

--------------- FINAL CTE -------------------------
final as (
  select
    *
  from transform)
select * from final