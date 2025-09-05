-------------- IMPORTS -----------------------
with 

dolar_exchange as (
  select * from {{ source('apis', 'lnd_api__dolar_exchange') }}),

--------------- CUSTOM LOGIC -------------------------
dates as (
    select explode(sequence(
        to_date('2025-01-01'),
        current_date(),
        interval 1 day
    )) as _date),


avg_dolar_exchange as (
  select
    date_trunc('week', reference_date) as reference_week,
    avg(exchange) as exchange
  from dolar_exchange
  group by 1),

--------------- FINAL CTE -------------------------
final as (
  select
    `_date` as reference_date,
    round(coalesce(dolar_exchange.exchange, avg_dolar_exchange.exchange),4) as exchange
  from dates
  left join dolar_exchange on dates.`_date` = dolar_exchange.reference_date
  left join avg_dolar_exchange on date_trunc('week', dates.`_date`) = avg_dolar_exchange.reference_week and dolar_exchange.exchange is null)
select * from final