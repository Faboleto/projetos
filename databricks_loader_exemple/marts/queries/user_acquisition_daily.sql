with

recursive date_array as (
    select '2023-01-01'::date as _date
    union all
    select (_date + interval '1 day')::date
    from date_array
    where _date + interval '1 day' <= CURRENT_DATE),

daily_acquisition as (
    select 
        created_at::date as _date,
        medium_agg,
        source,
        campaign_agg,
        sum(att.cpl_cost) as cost,
        count(*) as new_users,
        sum(logged_in) as logged_in,
        sum(deposited) as deposited,
        sum(activated) as activated_users,
        sum(value_spent_7d) as value_spent_7d,
        sum(value_received_7d) as value_received_7d,
        sum(total_engagement_7d) as total_engagement_7d,
        sum(net_value_7d) as net_value_7d,
        sum(value_spent_30d) as value_spent_30d,
        sum(value_received_30d) as value_received_30d,
        sum(total_engagement_30d) as total_engagement_30d,
        sum(net_value_30d) as net_value_30d,
        sum(value_spent_60d) as value_spent_60d,
        sum(value_received_60d) as value_received_60d,
        sum(net_value_60d) as net_value_60d,
        sum(value_spent_90d) as value_spent_90d,
        sum(value_received_90d) as value_received_90d,
        sum(net_value_90d) as net_value_90d
    from 
        looker.DIM_USER_campaign_source_v3 att
    where ban_status = 'Not Banned'
    group by 1, 2, 3, 4),

influencer_cost as (
    select 
        reference_date as _date,
        'influencer' as medium_agg,
        font as source,
        font as campaign_agg,
        sum(cost) as cost,
        0 as new_users,
        0 as logged_in,
        0 as deposited,
        0 as activated_users,
        0 as value_spent_7d,
        0 as value_received_7d,
        0 as total_engagement_7d,
        0 as net_value_7d,
        0 as value_spent_30d,
        0 as value_received_30d,
        0 as total_engagement_30d,
        0 as net_value_30d,
        0 as value_spent_60d,
        0 as value_received_60d,
        0 as net_value_60d,
        0 as value_spent_90d,
        0 as value_received_90d,
        0 as net_value_90d
    from looker.ads_cost_pivot_v2
    where font not in ('adwords', 'amazon', 'others', 'adwords_lol', 'adwords_tft', 'adwords_cs', 'facebook-ads')
    group by 1, 2, 3, 4),

daily_acquisition_and_costs as (
    (select * from daily_acquisition)
    union all
    (select * from influencer_cost)),

registration_frame as (
    select 
        dates._date,
        acq.medium_agg,
        acq.source,
        acq.campaign_agg,
        coalesce(sum(cost), 0) as cost,
        sum(new_users) as new_users,
        sum(logged_in) as logged_in,
        sum(deposited) as deposited,
        sum(activated_users) as activated_users,
        sum(value_spent_7d) as value_spent_7d,
        sum(value_received_7d) as value_received_7d,
        sum(total_engagement_7d) as total_engagement_7d,
        sum(net_value_7d) as net_value_7d,
        sum(value_spent_30d) as value_spent_30d,
        sum(value_received_30d) as value_received_30d,
        sum(total_engagement_30d) as total_engagement_30d,
        sum(net_value_30d) as net_value_30d,
        sum(value_spent_60d) as value_spent_60d,
        sum(value_received_60d) as value_received_60d,
        sum(net_value_60d) as net_value_60d,
        sum(value_spent_90d) as value_spent_90d,
        sum(value_received_90d) as value_received_90d,
        sum(net_value_90d) as net_value_90d
    from date_array dates
        left join daily_acquisition_and_costs acq on dates._date = acq._date
    group by 1, 2, 3, 4),

activation_frame as (
    select 
        first_coupon_at as _date 
        , medium_agg
        , source
        , campaign_agg
        , count(*) as activated_on_date
        , sum(lol_activated) as lol_activated_on_date
        , sum(valorant_activated) as valorant_activated_on_date
        , sum(aram_activated) as aram_activated_on_date
        , sum(tft_activated) as tft_activated_on_date
        , sum(cs_activated) as cs_activated_on_date
        , sum(other_activated) as other_activated_on_date
    from looker.DIM_USER_campaign_source_v3 att
    group by 1, 2, 3, 4),

final as (
    select 
        rf._date,
        rf.medium_agg,
        rf.source,
        rf.campaign_agg,
        rf.cost,
        rf.new_users,
        rf.logged_in,
        rf.deposited,
        rf.activated_users,
        coalesce(af.activated_on_date, 0) as activated_on_date,
        coalesce(af.lol_activated_on_date, 0) as lol_activated_on_date,
        coalesce(af.valorant_activated_on_date, 0) as valorant_activated_on_date,
        coalesce(af.aram_activated_on_date, 0) as aram_activated_on_date,
        coalesce(af.tft_activated_on_date, 0) as tft_activated_on_date,
        coalesce(af.cs_activated_on_date, 0) as cs_activated_on_date,
        coalesce(af.other_activated_on_date, 0) as other_activated_on_date,
        rf.value_spent_7d,
        rf.value_received_7d,
        rf.total_engagement_7d,
        rf.net_value_7d,
        rf.value_spent_30d,
        rf.value_received_30d,
        rf.total_engagement_30d,
        rf.net_value_30d,
        rf.value_spent_60d,
        rf.value_received_60d,
        rf.net_value_60d,
        rf.value_spent_90d,
        rf.value_received_90d,
        rf.net_value_90d
    from registration_frame rf
        left join activation_frame af on af._date = rf._date and af.medium_agg = rf.medium_agg and af.source = rf.source and af.campaign_agg = rf.campaign_agg)
select * from final