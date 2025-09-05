-------------- CONFIG -----------------------
{{config(
    post_hook=[
      "ALTER TABLE {{ this }} CLUSTER BY (gamemode, reference_date)",
      "OPTIMIZE {{ this }}"]
)}}

-------------- IMPORTS -----------------------
with 

users_blocklist as (
    select * from {{ ref('stg_db__users_blocklist') }}),

coupons_finished as (
    select * from {{ ref('fact__coupons_finished') }}),

league_points as (
    select * from {{ ref('stg_db__league_points') }}),

coupon_items as (
    select * from {{ ref('stg_db__coupon_items') }}),

challenges as (
    select * from {{ ref('stg_db__challenges') }}),

users as (
    select * from {{ ref('stg_db__users') }}),

transactions_finished as (
    select * from {{ ref('fact__transaction_items_artificial') }}),

--------------- CUSTOM LOGIC -------------------------

gamemodes_played_by_users as (
    select 
        user_id, 
        gamemode 
    from coupons_finished
    group by 1,2 
    order by 1,2 asc),

leagues_gamemode as (
    select 
        lp.league_id,
        cc.external_gamemode as gamemode
    from league_points  lp
        left join coupon_items ci on lp.external_match_id = ci.external_match_id
        left join challenges cc on ci.challenge_id = cc.challenge_id
    group by 1,2),

league_entrance as (
    select 
        cast(created_at as date) as reference_date,
        tf.user_id,
        coalesce(lg.gamemode, 'LOL1V1') as gamemode,
        null as coupon_id,
        transaction_id,
        cast(null as int) as bet_amount,
        absolut_amount as amount,
        'LEAGUE_ENTRANCE' as type
    from transactions_finished tf
        left join leagues_gamemode lg on tf.league_id = lg.league_id
    where transaction_type in ('LEAGUE_REGISTRATION', 'LEAGUE_TICKET')
    and debit_credit = 'CREDIT'
    and wallet_currency = 'BRL'
    and cast(created_at as date) >= '2024-01-01'),

subscription as (
    select 
        cast(tf.created_at as date) as reference_date,
        tf.user_id,
        null as gamemode,
        null as coupon_id,
        transaction_id,
        cast(null as int) as bet_amount,
        absolut_amount as amount,
        'SUBSCRIPTION_OLD_USER' as type
    from transactions_finished tf
        left join staging.stg_db__users u on tf.user_id = u.user_id
    where transaction_type = 'SUBSCRIPTION_OLD_USER'
    and tf.debit_credit = 'CREDIT'
    and u.created_at >= '2023-10-11'),

coupons_revenue as (
    select
        cast(ti.created_at as date) as reference_date,
        cf.user_id,
        gamemode,
        ti.coupon_id,
        transaction_id,
        bet_amount as bet_amount,
        ti.amount_rev as amount,
        case 
            when ti.debit_credit = 'ARTIFICIAL' then 'RECOVERED_REVENUE'
            when ti.debit_credit = 'ARTIFICIAL_UNBANNED' then 'ARTIFICIAL_UNBANNED'
            when ti.debit_credit != 'ARTIFICIAL' then ti.transaction_type
        end  as type
    from transactions_finished ti
        left join coupons_finished cf on ti.coupon_id = cf.coupon_id
    where transaction_type in ('COUPON', 'COUPON_REFUND')
    and wallet_currency = 'BRL'),

bonus_converted as (
    select
        cast(created_at as date) as reference_date,
        cf.user_id,
        gamemode,
        coupon_id,
        null as transaction_id,
        bet_amount as bet_amount,
        balance_amount as amount,
        'BONUS_CONVERTED' as type
    from coupons_finished cf
    where currency = 'BONUS_BRL'
    and coupon_status = 'WON'),

bonus_received as (
    select 
        cast(created_at as date) as reference_date,
        tf.user_id,
        null as gamemode,
        null as coupon_id,
        transaction_id,
        cast(null as int) as bet_amount,
        absolut_amount as amount,
        'BONUS' as type
    from transactions_finished tf
    where wallet_currency = 'BONUS_BRL'
    and transaction_type in ('ROLLOVER', 'BONUS', 'LEAGUE_PRIZE', 'COUPON_REFUND', 'BACKOFFICE_BONUS')
    and debit_credit = 'DEBIT'
    order by created_at desc),

payin_payout as (
    select 
        cast(updated_at as date) as reference_date,
        tf.user_id,
        null as gamemode,
        null as coupon_id,
        transaction_id,
        cast(null as int) as bet_amount,
        absolut_amount as amount,
        transaction_type as type
    from transactions_finished tf
    where transaction_type in('PAYIN', 'PAYOUT')),

--------------- ADD GAMEMODE -------------------------

subscription_gamemode as (
    select
        reference_date,
        s.user_id,
        coalesce(g.gamemode, 'LOL1V1') as gamemode,
        coupon_id,
        transaction_id,
        bet_amount,
        amount,
        type
    from subscription s 
        left join gamemodes_played_by_users g on s.user_id = g.user_id),

bonus_received_gamemode as (
    select 
        reference_date,
        br.user_id,
        coalesce(g.gamemode, 'LOL1V1') as gamemode,
        coupon_id,
        transaction_id,
        bet_amount,
        amount,
        type
    from bonus_received br 
        left join gamemodes_played_by_users g on br.user_id = g.user_id),

payin_payout_gamemode as (
    select 
        reference_date,
        pp.user_id,
        coalesce(g.gamemode, 'LOL1V1') as gamemode,
        coupon_id,
        transaction_id,
        bet_amount,
        amount,
        type 
    from payin_payout pp 
        left join gamemodes_played_by_users g on pp.user_id = g.user_id),


--------------- UNIFY -------------------------
unified_general_sources as (
    select * from league_entrance
        union all
    select * from subscription
        union all
    select * from coupons_revenue
        union all
    select * from bonus_converted
        union all
    select * from bonus_received
        union all
    select * from payin_payout),

unified_gamemode_sources as (
    select * from league_entrance
        union all
    select * from subscription_gamemode
        union all
    select * from coupons_revenue
        union all
    select * from bonus_converted
        union all
    select * from bonus_received_gamemode
        union all
    select * from payin_payout_gamemode),

--------------- CALCULATING RESULTS -------------------------
general_results as (
    select 
        reference_date,
        'GENERAL' as gamemode,
        round(sum(case when  type = 'LEAGUE_ENTRANCE' then amount else 0 end),2) as league_entrance,
        round(sum(case when  type = 'SUBSCRIPTION_OLD_USER' then amount else 0 end),2) as subscription_old_user,
        round(sum(case when  type in ('COUPON', 'RECOVERED_REVENUE','COUPON_REFUND', 'ARTIFICIAL_UNBANNED') then amount else 0 end),2) as coupons,
		round(sum(case when  type = 'RECOVERED_REVENUE' then amount else 0 end),2) as recovered_revenue,
		round(sum(case when  type in ('COUPON') then amount else 0 end),2) as coupons_only,
        round(sum(case when  type in ('ARTIFICIAL_UNBANNED') then amount else 0 end),2) as unbanned_users,
        round(sum(case when  type = 'COUPON_REFUND' then amount else 0 end),2) as coupons_refund,
        round(sum(case when  type = 'BONUS_CONVERTED' then amount else 0 end),2) as bonus_converted,
        round(sum(case when  type = 'BONUS_CONVERTED' then abs(amount) else 0 end),2) as bonus_converted_abs,
        round(sum(case when  type = 'BONUS' then amount else 0 end),2) as bonus_received,
        round(sum(case when  type = 'PAYIN' then amount else 0 end),2) as payin,
        round(sum(case when  type = 'PAYOUT' then amount else 0 end),2) as payout_abs,
        round(sum(case when  type = 'PAYOUT' then -(amount) else 0 end),2) as payout,
        round(sum(case when  type = 'PAYIN' then amount else 0 end) - sum(case when  type = 'PAYOUT' then amount else 0 end),2) as payin_payout_balance
    from unified_general_sources
    group by 1,2),

gamemode_results as (
    select 
        reference_date,
        gamemode,
        round(sum(case when  type = 'LEAGUE_ENTRANCE' then amount else 0 end),2) as league_entrance,
        round(sum(case when  type = 'SUBSCRIPTION_OLD_USER' then amount else 0 end),2) as subscription_old_user,
        round(sum(case when  type in ('COUPON', 'RECOVERED_REVENUE','COUPON_REFUND','ARTIFICIAL_UNBANNED') then amount else 0 end),2) as coupons,
		round(sum(case when  type = 'RECOVERED_REVENUE' then amount else 0 end),2) as recovered_revenue,
		round(sum(case when  type in ('COUPON') then amount else 0 end),2) as coupons_only,
        round(sum(case when  type in ('ARTIFICIAL_UNBANNED') then amount else 0 end),2) as unbanned_users,
        round(sum(case when  type = 'COUPON_REFUND' then amount else 0 end),2) as coupons_refund,
        round(sum(case when  type = 'BONUS_CONVERTED' then amount else 0 end),2) as bonus_converted,
        round(sum(case when  type = 'BONUS_CONVERTED' then abs(amount) else 0 end),2) as bonus_converted_abs,
        round(sum(case when  type = 'BONUS' then amount else 0 end),2) as bonus_received,
        round(sum(case when  type = 'PAYIN' then amount else 0 end),2) as payin,
        round(sum(case when  type = 'PAYOUT' then amount else 0 end),2) as payout_abs,
        round(sum(case when  type = 'PAYOUT' then -(amount) else 0 end),2) as payout,
        round(sum(case when  type = 'PAYIN' then amount else 0 end) - sum(case when  type = 'PAYOUT' then amount else 0 end),2) as payin_payout_balance
    from unified_gamemode_sources
    group by 1,2),

unified_results as (
    select * from general_results
        union all
    select * from gamemode_results),

monthly_users as (
    select 
        cast(date_trunc('month', created_at) as date) as created_at,
        'GENERAL' as gamemode,
        count(distinct fc.user_id) as monthly_users
    from coupons_finished fc
    group by 1,2),

monthly_users_per_gamemode as (
    select 
        cast(date_trunc('month', created_at) as date) as created_at,
        fc.gamemode,
        count(distinct fc.user_id) as monthly_users
    from coupons_finished fc
    group by 1,2),

results_and_users as (
    select
        ur.*,
        case
            when ur.gamemode = 'GENERAL' then mu.monthly_users
            else mug.monthly_users end as monthly_users
    from unified_results ur
    left join monthly_users_per_gamemode mug on date_trunc('month', ur.reference_date) = mug.created_at and ur.gamemode = mug.gamemode
    left join monthly_users mu on date_trunc('month', ur.reference_date) = mu.created_at and ur.gamemode = mu.gamemode),

--------------- FINAL CTE -------------------------
final as (
    select 
        reference_date,
        gamemode,
        league_entrance,
        subscription_old_user,
        coupons,
		recovered_revenue,
		coupons_only,
        unbanned_users,
        coupons_refund,
        bonus_converted,
        bonus_converted_abs,
        bonus_received,
        payin,
        payout,
        payout_abs,
        payin_payout_balance,
        monthly_users
    from results_and_users)
select * from final 