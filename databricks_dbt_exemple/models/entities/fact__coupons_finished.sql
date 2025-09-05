-------------- CONFIG -----------------------
{{config(
    materialized="table",
    post_hook=[
      "ALTER TABLE {{ this }} CLUSTER BY (gamemode, coupon_id, user_id, external_match_id)",
      "OPTIMIZE {{ this }}"]
)}}

-------------- IMPORTS -----------------------
with 

coupons as (
  select * from {{ ref('stg_db__coupons') }}),

grouped_challenges as (
  select * from {{ ref('stg_db__grouped_challenges') }}),

users_blocklist as (
  select * from {{ ref('dim__users_banned') }}),

transactions as (
  select * from {{ ref('stg_db__transactions') }}),

transaction_items as (
  select * from {{ ref('stg_db__transaction_items') }}),

wallets as (
  select * from {{ ref('stg_db__wallets') }}),

accounts as (
  select * from {{ ref('stg_db__accounts') }}),

super_tips as (
  select * from {{ ref('stg_db__super_tips') }}),

coupon_items as (
  select * from {{ ref('stg_db__coupon_items') }}),

reputations as (
  select * from {{ ref('stg_db__reputation_scores') }}),

ext_match_player as (
  select * from {{ ref('stg_db__external_match_players') }}),

ext_players as (
  select * from {{ ref('stg_db__external_players') }}),

--------------- CUSTOM LOGIC -------------------------
coupons_currency as (
    select 
        coupon_id,
        concat_ws(', ', collect_list(w.currency)) as currency
    from transactions t
        left join transaction_items ti on t.transaction_id = ti.transaction_id
        left join wallets w on ti.wallet_id = w.wallet_id
        left join accounts a on w.account_id = a.account_id
        where a.type = 'PLAYER_ACCOUNT'
        and t.type = 'COUPON'
        and ti.status = 'CREDIT'
    group by 1),

get_external_match_id as (
    select 
        coupon_id,
        external_match_id,
        row_number() over (partition by coupon_id order by created_at asc) as match_rank
    from coupon_items),

coupons_external_match as (
    select 
        coupon_id,
        external_match_id
    from get_external_match_id
    where match_rank = 1),

match_player_user as (
  select 
    emp.*,
    ep.user_id,
    ep.external_gamemode
  from ext_match_player emp 
    left join ext_players ep on emp.external_player_id = ep.external_player_id),

--------------- FINAL CTE -------------------------
final as (
    select 
        c.coupon_id,
        c.user_id,
        c.created_at,
        gc.name as challenge_name,
        round(c.odd,2) as odd,
        c.type as coupon_type,
        round(c.amount, 2) as bet_amount,
        round(case when c.coupon_status = 'WON' then -(c.amount * (c.odd - 1)) else c.amount end,2) as balance_amount,
        round(case when c.coupon_status = 'WON' then (c.amount * (c.odd - 1)) else -c.amount end,2) as user_balance_amount,
        case
          when c.coupon_status = 'WON' and cc.currency = 'BONUS_BRL' then 0-(c.amount*(c.odd-1))
          when c.coupon_status = 'LOST' and cc.currency = 'BONUS_BRL' then 0
          when c.coupon_status = 'WON' and  cc.currency = 'BRL' then 0-(c.amount*(c.odd-1))
          when c.coupon_status = 'LOST' and cc.currency = 'BRL' then c.amount
          when c.coupon_status = 'REFUND' then -(c.amount)
          else null
        end balance_amount_bonus,
        case
          when c.coupon_status = 'WON' and cc.currency = 'BONUS_BRL' then (c.amount*(c.odd-1))
          when c.coupon_status = 'LOST' and cc.currency = 'BONUS_BRL' then 0
          when c.coupon_status = 'WON' and  cc.currency = 'BRL' then (c.amount*(c.odd-1))
          when c.coupon_status = 'LOST' and cc.currency = 'BRL' then -c.amount
          when c.coupon_status = 'REFUND' then -(c.amount)
          else null
        end user_balance_amount_bonus,
        cc.currency,
        c.coupon_status,
        c.rollover_id,
        c.grouped_challenge_id,
        coalesce(gc.gamemode,gc.external_gamemode) as gamemode,
        case when bn.user_id is not null then true else false end as is_banned,
        case when st.coupon_id is not null then true else false end as is_supertip,
        cem.external_match_id,
        c.origin,
        rep.max_amount,
        rep.metadata as reputation_metadata
    from coupons c
        left join grouped_challenges gc on c.grouped_challenge_id = gc.grouped_challenge_id
        left join coupons_currency cc on c.coupon_id = cc.coupon_id
        left join users_blocklist bn on c.user_id = bn.user_id
        left join super_tips st on c.coupon_id = st.coupon_id
        left join coupons_external_match cem on c.coupon_id = cem.coupon_id
        left join match_player_user mpu on cem.external_match_id = mpu.external_match_id and c.user_id = mpu.user_id and gc.external_gamemode = mpu.external_gamemode
        left join reputations rep on rep.external_match_player_id = mpu.external_match_player_id
    where c.coupon_status in ('WON', 'LOST', 'REFUND'))
select * from final