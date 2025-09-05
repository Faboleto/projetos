-------------- IMPORTS -----------------------
with 

gateway_transactions as (

  select * from {{ ref('stg_db__gateway_transactions') }}),

transactions_finished as (
  select * from {{ ref('fact__transaction_items_finished') }}),

accounts as (
  select * from {{ ref('stg_db__accounts') }}),

users_blocklist as (
  select * from {{ ref('stg_db__users_blocklist') }}),

user_notes as (
  select * from {{ ref('stg_db__user_notes') }}),

--------------- CUSTOM LOGIC -------------------------
finished_payouts as (
    select 
        user_id,
        created_at,
        transaction_id,
        absolut_amount
    from transactions_finished
    where transaction_type = 'PAYOUT'),

attempted_payouts as (
    select 
        a.user_id,
        gt.created_at,
        gt.transaction_id,
        gt.amount,
        gt.status
    from gateway_transactions gt
        left join accounts a on gt.account_id = a.account_id
    where gateway_name = 'STARKBANK'
    and gt.status != 'PAID'),

blocked_users as (
    select 
        user_blocklist_id as block_id,
        ub.user_id,
        ub.created_at,
        ub.description,
        un.notes,
        un.flag,
        un.cheat as is_cheating,
        un.smurf as is_smurfing,
        un.derank as is_deranking,
        un.duo_boost as is_duo_boosting,
        un.slow_climb as is_slow_climbing,
        un.gamemode,
        ub.active_ban,
        ub.expiry as expire_at,
        row_number() over (partition by ub.user_id order by ub.created_at desc) as last_ban
    from users_blocklist ub
        left join user_notes un on ub.user_notes_id = un.user_notes_id
    where ub.active_ban is true),

users_with_successful_payouts as (
    select 
        bu.block_id,
        bu.user_id,
        bu.created_at,
        count(distinct fp.transaction_id) as successful_payouts_in_2_weeks,
        coalesce(sum(fp.absolut_amount),0)::float as successful_payouts_amount
    from blocked_users bu
         left join finished_payouts fp on bu.user_id = fp.user_id and fp.created_at >= bu.created_at - interval '14 days' and fp.created_at <= bu.created_at
    group by 1,2,3
    order by bu.created_at desc),

users_with_payout_attempts as (
    select 
        bu.block_id,
        bu.user_id,
        bu.created_at,
        count(distinct ap.transaction_id) as attempts_payouts_before_ban,
        coalesce(sum(ap.amount),0)::float as attempted_amount
    from blocked_users bu
        left join attempted_payouts ap on bu.user_id = ap.user_id and ap.created_at >= bu.created_at - interval '3 days' and ap.created_at <= bu.created_at
    group by 1,2,3
    order by bu.created_at desc),

--------------- FINAL CTE -------------------------
final as (
    select 
        bu.block_id,
        bu.user_id,
        bu.created_at,
        bu.expire_at,
        bu.description,
        bu.notes,
        bu.flag,
        bu.is_cheating,
        bu.is_smurfing,
        bu.is_deranking,
        bu.is_duo_boosting,
        bu.is_slow_climbing,
        bu.gamemode,
        bu.active_ban,
        successful_payouts_in_2_weeks,
        successful_payouts_amount,
        attempts_payouts_before_ban,
        attempted_amount
    from blocked_users bu
        left join users_with_successful_payouts fp on bu.user_id = fp.user_id and fp.block_id = bu.block_id
        left join users_with_payout_attempts ap on bu.user_id = ap.user_id and ap.block_id = bu.block_id
    where last_ban = 1
    order by bu.user_id, bu.created_at desc)
select * from final