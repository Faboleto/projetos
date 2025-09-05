with

id_graph as (
    select distinct on (segment_id)
        segment_id,
        canonical_segment_id
    from segment_unify_prod.id_graph_updates 
    order by segment_id, timestamp desc),

channels (search, social) as (
    values(
        'google\.com|google\.android|bing\.com|yahoo\.com|brave\.com|duckduckgo\.com',
        'youtube\.com|linkedin\.com|lnkd\.in|instagram\.com|facebook\.com|twitch\.tv|steamcommunity\.com|//t\.co|tiktok\.com'
    )),

classified_visits_in_att_window as (
    select 
        users.user_id,
        pages.timestamp,
        case 
            when pages.context_campaign_source in ('influencer', 'influencers') then 'influencer'
            when pages.context_campaign_medium is not null then pages.context_campaign_medium
            when pages.path like '%blog%' then 'search-blog'
            when (select x[1] from regexp_matches(pages.referrer, (select '(' || search || ')' from channels)) x) is not null then 'search-brand'
            when (select x[1] from regexp_matches(pages.referrer, (select '(' || social || ')' from channels)) x) is not null then 'social'
            when pages.referrer is null then 'direct'
        else 'referral' end as medium,
        case 
            when pages.context_campaign_source in ('influencer', 'influencers') then dcs.influencer
            when pages.context_campaign_source is not null then pages.context_campaign_source
            when pages.path like '%blog%' then pages.path
            when (select x[1] from regexp_matches(pages.referrer, (select '(' || search || social || ')' from channels)) x) is not null
                then (select x[1] from regexp_matches(pages.referrer,  (select '(' || search || social || ')' from channels)) x)
            when pages.referrer is null then 'direct'
        else pages.referrer end as source, 
        pages.path,
        pages.referrer,
        coalesce(
            pages.context_campaign_name,
            case when pages.path like '%blog%' then pages.path else null end) as campaign,
        context_campaign_content as content,
        pages.context_campaign_term as term,
        0 as mgm_cost
    from 
        segment_unify_prod.pages pages
        left join looker.dim_campaign_source dcs on pages.context_campaign_name = dcs.source_name
        inner join id_graph graph on graph.segment_id = pages.segment_id
        inner join segment_unify_prod.user_identifiers identifiers on identifiers.canonical_segment_id = graph.canonical_segment_id
            and identifiers.type = 'user_id'
        inner join public.users users on users.user_id = identifiers.value
    where
        coalesce(referrer, '') not like '%#####.%'
        and (pages.timestamp between users.created_at - interval '30 days' and users.created_at)),

mgm_attribution as (
    select distinct on (referred_id)
        referred_id as user_id,
        referrals.created_at as timestamp,
        'mgm' as medium,
        referrer.email as source,
        null as path,
        null as referrer,
        null as campaign,
        null as content,
        null as term,
        case when status = 'FINISHED' then reward else 0 end as mgm_cost
    from public.referrals referrals
        join public.users referrer on referrer.user_id = referrals.referrer_id
    order by referrals.referred_id, referrals.created_at desc),

users_att_events as (
    (select * from classified_visits_in_att_window)
    union all 
    (select * from mgm_attribution)),

users_att_events_ranked as (
    select 
        *,
        case
            when medium in ('mgm') then 1
            when medium in ('paid-media', 'site') then 2
            when medium in ('influencers', 'influencer', 'cpv') then 3
            when medium not in ('direct', 'search-brand', 'search-blog', 'referral') then 4
            when medium in ('search-brand', 'search-blog') then 5
            when medium in ('referral') then 6
            when medium in ('direct') then 7
        else null end as medium_priority,
        row_number() over (partition by user_id order by timestamp desc) as visit_priority
    from users_att_events),

users_att as (
    select distinct on (user_id)
        user_id,
        medium,
        source,
        referrer,
        path as page_path,
        campaign,
        content,
        term,
        mgm_cost
    from users_att_events_ranked
    order by user_id, medium_priority asc, visit_priority asc),

users_att_agg_medium as (
    select
        *,
        case 
            when medium in ('google-ads', 'paid-media', 'site', 'youtube') then 'paid-media'
            when medium in ('influencer', 'cpv') then 'influencer' 
        else medium end as medium_agg,
        case 
            when medium in ('influencer', 'mgm', 'social', 'search-brand', 'search-blog') then source
        else campaign end as campaign_agg
    from users_att),

users_attributed_final as (
    select 
        u.user_id,
        (u.created_at::timestamptz at time zone 'America/Sao_Paulo')::date as created_at,
        case when (u.first_login::date - u.created_at::date) <= 7 then 1 else 0 end as logged_in,
        coalesce(att.medium, 'None') as medium,
        coalesce(att.medium_agg, 'None') as medium_agg,
        coalesce(att.source, 'None') as source,
        coalesce(att.referrer, 'None') as referrer,
        coalesce(att.page_path, 'None') as page_path,
        coalesce(att.campaign, 'None') as campaign,
        coalesce(att.campaign_agg, 'None') as campaign_agg,
        coalesce(att.content, 'None') as content,
        coalesce(att.term, 'None') as term,
        coalesce(att.mgm_cost, 0) as mgm_cost
    from 
        public.users u
        left join users_att_agg_medium att on u.user_id = att.user_id
    where u.user_id not in ('bab2a9d2-5968-42d0-ab0c-5ee7251f6829')),


first_coupon_at as (
    select distinct on (coupons.user_id)
        user_id,
        coupons.created_at as first_coupon_at,
        coalesce(cha.external_gamemode, cha.gamemode) as first_coupon_gamemode
    from coupon_items ci
        left join coupons on coupons.coupon_id = ci.coupon_id
        left join challenges cha on cha.challenge_id = ci.challenge_id
    where 1=1
        and coalesce(coupons.status, '') in ('WON', 'LOST', 'REFUND')
        and coupons.type != 'FREE' 
    order by coupons.user_id, coupons.created_at asc),

first_deposit_at as (
    select user_id, min(reference_date) as first_deposit_at from looker.fact_user_deposits group by 1),

users_attributed_activation_final as (
    select 
        att.*,
        case when (fda.first_deposit_at::date - att.created_at::date) <= 7 then 1 else 0 end as deposited,
        case when (fca.first_coupon_at::date - att.created_at::date) <= 7 then 1 else 0 end as activated,
        fca.first_coupon_at::date as first_coupon_at,
        case 
            when fca.first_coupon_gamemode in ('LOLEXTERNAL5V5', 'LOL1V1', 'LOL2V2', 'LOLEXTERNALBLITZ') then 'LOL'
            when fca.first_coupon_gamemode in ('VALEXTERNALRANKED') then 'VALORANT'
            when fca.first_coupon_gamemode in ('LOLEXTERNALARAM') then 'ARAM'
            when fca.first_coupon_gamemode in ('TFTEXTERNALRANKED') then 'TFT'
            when fca.first_coupon_gamemode in ('CS2EXTERNALRANKED', 'CS2EXTERNALFACEIT') then 'CS2'
            when fca.first_coupon_gamemode is null then 'None'
        else 'Outros' end as first_coupon_gamemode,
        coalesce(fca.first_coupon_gamemode, 'None') as original_gamemode
    from users_attributed_final att
      left join first_coupon_at fca on att.user_id = fca.user_id
      left join first_deposit_at fda on att.user_id = fda.user_id),

facebook_ads as (
    select 
        reports.date_start::date as _date,
        fb_campaigns.id as campaign,
        sum(reports.spend) as cost
    from segment__facebook_ads.insights reports
        left join  segment__facebook_ads.ads fb_ads on fb_ads.id = reports.ad_id 
        left join segment__facebook_ads.campaigns fb_campaigns on fb_campaigns.id = fb_ads.campaign_id
    group by 1, 2),

facebook_ads_cpl_not_tracked as (
    select 
        reports.date_start::date as _date,
        sum(reports.spend) as cost
    from segment__facebook_ads.insights reports
        left join  segment__facebook_ads.ads fb_ads on fb_ads.id = reports.ad_id 
        left join segment__facebook_ads.campaigns fb_campaigns on fb_campaigns.id = fb_ads.campaign_id
        left join users_attributed_activation_final att on att.created_at = reports.date_start::date and att.source = 'facebook-ads' and att.campaign = fb_campaigns.id
    where att.user_id is null
    group by 1),

facebook_ads_cac_not_tracked as (
    select 
        reports.date_start::date as _date,
        sum(reports.spend) as cost
    from segment__facebook_ads.insights reports
        left join  segment__facebook_ads.ads fb_ads on fb_ads.id = reports.ad_id 
        left join segment__facebook_ads.campaigns fb_campaigns on fb_campaigns.id = fb_ads.campaign_id
        left join users_attributed_activation_final att on att.first_coupon_at = reports.date_start::date and att.source = 'facebook-ads' and att.campaign = fb_campaigns.id
    where att.user_id is null
    group by 1),

google_ads as (
    select 
        date_trunc('day', date_stop)::date as _date,
        campaign_id as campaign,
        round(sum(cost/1000000), 2) as cost
    from segment_google_ads_#####.campaign_performance_reports reports
    group by 1, 2),

google_ads_cpl_not_tracked as (
    select 
        date_trunc('day', date_stop)::date as _date,
        round(sum(cost/1000000), 2) as cost
    from segment_google_ads_#####.campaign_performance_reports reports
        left join users_attributed_activation_final att on att.created_at = reports.date_stop::date and att.source = 'adwords' and att.campaign = reports.campaign_id
    where att.user_id is null
    group by 1),

google_ads_cac_not_tracked as (
    select 
        date_trunc('day', date_stop)::date as _date,
        round(sum(cost/1000000), 2) as cost
    from segment_google_ads_#####.campaign_performance_reports reports
        left join users_attributed_activation_final att on att.first_coupon_at = reports.date_stop::date and att.source = 'adwords' and att.campaign = reports.campaign_id
    where att.user_id is null
    group by 1),

paid_media_tracked_cpl as (
    select 
        att.created_at,
        att.medium_agg,
        att.source,
        att.campaign,
        count(distinct user_id) as users,
        round(coalesce(avg(google_ads.cost), 0) + coalesce(avg(fb_ads.cost), 0), 2) as campaign_cost,
        round(coalesce(avg(google_ads.cost), 0) + coalesce(avg(fb_ads.cost), 0), 2)/count(distinct user_id) as cpl
    from users_attributed_activation_final att
        left join google_ads on att.source = 'adwords' and google_ads.campaign = att.campaign and att.created_at = google_ads._date
        left join facebook_ads fb_ads on att.source = 'facebook-ads' and fb_ads.campaign = att.campaign and att.created_at = fb_ads._date
    where medium_agg = 'paid-media'
    group by 1, 2, 3, 4),

cpl_tracked_users as (
    select 
        created_at,
        medium_agg,
        source,
        sum(users) as users
    from paid_media_tracked_cpl
    group by 1, 2, 3),

cpl_excess_amount as (
    select 
        cpl.created_at,
        cpl.medium_agg,
        cpl.source,
        (coalesce(gads.cost, 0 ) + coalesce(fbads.cost, 0))/users as excess_cpl
    from cpl_tracked_users cpl
        left join google_ads_cpl_not_tracked gads on gads._date = cpl.created_at and cpl.medium_agg = 'paid-media' and cpl.source ='adwords'
        left join facebook_ads_cpl_not_tracked fbads on fbads._date = cpl.created_at and cpl.medium_agg = 'paid-media' and cpl.source = 'facebook-ads'),

paid_media_corrected_cpl as (
    select 
        tracked.created_at,
        tracked.medium_agg,
        tracked.source,
        tracked.campaign, 
        tracked.users,
        tracked.campaign_cost,
        cpl + coalesce(excess_cpl, 0) as cpl
    from paid_media_tracked_cpl tracked
        left join cpl_excess_amount excess on excess.created_at = tracked.created_at and excess.medium_agg = tracked.medium_agg and excess.source = tracked.source),

paid_media_tracked_cac as (
    select 
        att.first_coupon_at,
        att.medium_agg,
        att.source,
        att.campaign,
        count(distinct user_id) as users,
        round(coalesce(avg(google_ads.cost), 0) + coalesce(avg(fb_ads.cost), 0), 2) as campaign_cost,
        round(coalesce(avg(google_ads.cost), 0) + coalesce(avg(fb_ads.cost), 0), 2)/count(distinct user_id) as cac
    from users_attributed_activation_final att
        left join google_ads on att.source = 'adwords' and google_ads.campaign = att.campaign and att.first_coupon_at = google_ads._date
        left join facebook_ads fb_ads on att.source = 'facebook-ads' and fb_ads.campaign = att.campaign and att.first_coupon_at = fb_ads._date
    where medium_agg = 'paid-media' and att.first_coupon_at is not null
    group by 1, 2, 3, 4),

cac_tracked_users as (
    select 
        first_coupon_at,
        medium_agg,
        source,
        sum(users) as users
    from paid_media_tracked_cac
    group by 1, 2, 3),

cac_excess_amount as (
    select 
        cac.first_coupon_at,
        cac.medium_agg,
        cac.source,
        (coalesce(gads.cost, 0 ) + coalesce(fbads.cost, 0))/users as excess_cac
    from cac_tracked_users cac
        left join google_ads_cac_not_tracked gads on gads._date = cac.first_coupon_at and cac.medium_agg = 'paid-media' and cac.source ='adwords'
        left join facebook_ads_cac_not_tracked fbads on fbads._date = cac.first_coupon_at and cac.medium_agg = 'paid-media' and cac.source = 'facebook-ads'),

paid_media_corrected_cac as (
    select 
        tracked.first_coupon_at,
        tracked.medium_agg,
        tracked.source,
        tracked.campaign, 
        tracked.users,
        tracked.campaign_cost,
        cac + coalesce(excess_cac, 0) as cac
    from paid_media_tracked_cac tracked
        left join cac_excess_amount excess on excess.first_coupon_at = tracked.first_coupon_at and excess.medium_agg = tracked.medium_agg and excess.source = tracked.source),


payments_and_gains as (
    select  
        users.user_id,
        t.created_at::date as transaction_date,
        t.created_at::date - users.created_at::date as days_since_user_creation,
        case 
            when t.transaction_balance = 'CREDIT' and w.currency = 'BRL'  then 'PAYMENT'
            when t.transaction_balance = 'CREDIT' and w.currency = 'BONUS_BRL' then 'BONUS_USE'
            when t.transaction_balance = 'DEBIT' and w.currency = 'BRL' then 'GAIN'
        else null end as type,
        t.transaction_amount as value
    from users 
        left join accounts on users.user_id = accounts.user_id
        left join wallet w on accounts.account_id = w.account_id
        left join transactions t on w.wallet_id = t.wallet_id
    where t.transaction_type in ('LEAGUE_TICKET', 'LEAGUE_REGISTRATION','COUPON', 'COUPON_REFUND', 'BUG_AMOUNT', 'MATCH')),

users_egagement as (
    select 
        user_id,
        sum(value) filter(where type = 'PAYMENT' and days_since_user_creation <= 7) as value_spent_7d,
        sum(value) filter(where type = 'GAIN' and days_since_user_creation <= 7) as value_received_7d,
        sum(value) filter(where type in ('PAYMENT', 'BONUS_USE') and days_since_user_creation <= 7) as total_engagement_7d,
        sum(value) filter(where type = 'PAYMENT' and days_since_user_creation <= 30) as value_spent_30d,
        sum(value) filter(where type = 'GAIN' and days_since_user_creation <= 30) as value_received_30d,
        sum(value) filter(where type = 'PAYMENT' and days_since_user_creation <= 30) as total_engagement_30d,
        sum(value) filter(where type = 'PAYMENT' and days_since_user_creation <= 60) as value_spent_60d,
        sum(value) filter(where type = 'GAIN' and days_since_user_creation <= 60) as value_received_60d,
        sum(value) filter(where type = 'PAYMENT' and days_since_user_creation <= 90) as value_spent_90d,
        sum(value) filter(where type = 'GAIN' and days_since_user_creation <= 90) as value_received_90d
    from payments_and_gains
    group by 1),

banned as  (
    select distinct on (blocklist.user_id)
        blocklist.user_id,
        case when cast(blocklist.expiry as date) - cast(now() as date) > 0 then true else false end as active_ban
    from user_blocklist blocklist
    order by blocklist.user_id, blocklist.created_at desc),

final as (
    select 
        att.user_id,
        case when bans.active_ban is true then 'Banned' else 'Not Banned' end as ban_status,
        att.created_at,
        att.logged_in,
        att.medium,
        att.medium_agg,
        att.source,
        att.referrer,
        att.page_path,
        att.campaign,
        att.campaign_agg,
        att.content,
        att.term,
        att.deposited,
        att.activated,
        att.first_coupon_at,
        att.first_coupon_gamemode,
        att.original_gamemode,
        case when att.first_coupon_gamemode = 'LOL' then 1 else 0 end as lol_activated,
        case when att.first_coupon_gamemode = 'ARAM' then 1 else 0 end as aram_activated,
        case when att.first_coupon_gamemode = 'TFT' then 1 else 0 end as tft_activated,
        case when att.first_coupon_gamemode = 'CS2' then 1 else 0 end as cs_activated,
        case when att.first_coupon_gamemode = 'VALORANT' then 1 else 0 end as valorant_activated,
        case when att.first_coupon_gamemode = 'Other' then 1 else 0 end as other_activated,
        att.mgm_cost + coalesce(cpl.cpl, 0) as cpl_cost,
        att.mgm_cost + coalesce(cac.cac, 0) as cac_cost,
        coalesce(_stats.value_spent_7d, 0) as value_spent_7d,
        coalesce(_stats.value_received_7d , 0) as value_received_7d,
        coalesce(_stats.total_engagement_7d, 0) as total_engagement_7d,
        coalesce(_stats.value_spent_7d - _stats.value_received_7d, 0) as net_value_7d,
        coalesce(_stats.value_spent_30d, 0) as value_spent_30d,
        coalesce(_stats.value_received_30d, 0) as value_received_30d,
        coalesce(_stats.total_engagement_30d, 0) as total_engagement_30d,
        coalesce(_stats.value_spent_30d - _stats.value_received_30d, 0) as net_value_30d,
        coalesce(_stats.value_spent_60d, 0) as value_spent_60d,
        coalesce(_stats.value_received_60d, 0) as value_received_60d,
        coalesce(_stats.value_spent_60d - _stats.value_received_60d, 0) as net_value_60d,
        coalesce(_stats.value_spent_90d, 0) as value_spent_90d,
        coalesce(_stats.value_received_90d, 0) as value_received_90d,
        coalesce(_stats.value_spent_90d - _stats.value_received_90d, 0) as net_value_90d
    from 
        users_attributed_activation_final att
        left join users_egagement _stats on _stats.user_id = att.user_id
        left join banned bans on att.user_id = bans.user_id
        left join paid_media_corrected_cpl cpl on att.created_at = cpl.created_at and att.medium_agg = cpl.medium_agg and att.source = cpl.source and att.campaign = cpl.campaign
        left join paid_media_corrected_cac cac on att.first_coupon_at = cac.first_coupon_at and att.medium_agg = cac.medium_agg and att.source = cac.source and att.campaign = cac.campaign)
select * from final