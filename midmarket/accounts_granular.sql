-- Predecessor table: eda_analytics__prototype_views_gtm.cohort
-- Destination table: marketing_analyst_general.rw_mm_accounts_granular

with fraud_v2 as (
  	select 
    	distinct 
    	a.account_id,
    	cd.crm_account_id,
    	billing_batch,
    	z.accounttype__c as zuora_account_type
  	from pdw.accounts a 
  	join edw_consolidated.customer_dim_scd2 cd on split(cd.zendesk_account_id, ".")[offset(0)] = cast(a.account_id as string)
      	and cd.dw_curr_ind = "Y"
 -- join `pdw.derived_account_view`  d on d.account_id = a.account_id and d.latest_run_at = a.run_at
  	join `edw-prod-153420.zuora.account_scd2` z on a.account_id = SAFE_CAST(SAFE_CAST(z.zendeskaccountid__c AS FLOAT64) AS INT64) and z.dw_curr_ind = "Y"
  	where (billing_batch = "Batch13" or z.accounttype__c = "Fraudulent")  
  	group by 1,2,3,4
),

usage as (
  	select distinct 
    	run_at,
    	crm_account_id,
    	product_combo,
    	number_of_products,
    	paying_customer_ind,
		COALESCE(is_paying_support, 0) + COALESCE(is_paying_chat, 0) + COALESCE(is_paying_talk, 0) + COALESCE(is_paying_guide, 0) AS core_products_paid,
    	IF(COALESCE(is_activated_support, 0) = 1 AND COALESCE(is_paying_support,0)  = 1, 1, 0) +
    	IF(COALESCE(is_paying_guide, 0)      = 1 AND COALESCE(is_activated_guide,0) = 1, 1, 0) + 
    	IF(COALESCE(is_activated_chat, 0)    = 1 AND COALESCE(is_paying_chat, 0)    = 1, 1, 0) +
    	IF(COALESCE(is_activated_talk, 0)    = 1 AND COALESCE(is_paying_talk, 0)    = 1, 1, 0) AS core_products_activated,
    	case when is_engaged_support+is_engaged_chat+is_engaged_guide+is_engaged_talk = 4 then 4
		when is_engaged_support+is_engaged_chat+is_engaged_guide+is_engaged_talk = 3 then 3
		when is_engaged_support+is_engaged_chat+is_engaged_guide+is_engaged_talk = 2 then 2
		when is_engaged_support+is_engaged_chat+is_engaged_guide+is_engaged_talk = 1 then 1
		when is_engaged_support+is_engaged_chat+is_engaged_guide+is_engaged_talk = 0 then 0
		else 0
		end as num_engaged_products,
		case when is_engaged_support+is_engaged_chat+is_engaged_guide+is_engaged_talk >= 1 then 'Engaged'
		else 'Not Engaged'
		end as engaged_flag
  	from `edw-prod-153420.product_strategy__public.product_engagement_crm_master` p
  	join `edw_consolidated.edw_date_dim` b on p.run_at = b.the_date 
	where product_combo not in ('No Paid Products')
		--and run_at >= '2018-01-01' 
),

product_combo as (
	with crm_product as (
   	select
      gr.mrr_date
      , gr.crm_account_id
      , gr.product_line
      , MAX(pd.enterpriselicense_ind) AS ela_flag
      , STRING_AGG(DISTINCT gr.product_offering) AS offerings
      , STRING_AGG(DISTINCT CASE WHEN gr.product_offering LIKE '%Answer%' THEN 'Answer Bot' 
                                 WHEN pd.product_type_derived = 'Addon' THEN 'Add-on'
                                 ELSE gr.product_line END) AS product_combo
      , COUNT(DISTINCT CASE WHEN gr.product_offering LIKE '%Answer%' THEN 'Answer Bot' 
                            WHEN pd.product_type_derived = 'Addon' THEN 'Add-on'
                            ELSE gr.product_line END) AS all_product_count
      , COUNT(DISTINCT gr.product_line ) AS base_product_count
      , SUM(IF(gr.net_arr > 0 AND gr.product_line IN ('Support', 'Chat', 'Talk'), gr.baseplan_quantity, 0)) AS paid_agents
      , SUM(IF(gr.net_arr > 0 AND gr.product_line IN ('Support'), gr.baseplan_quantity, 0)) AS paid_support_agents
      , SUM(IF(gr.net_arr > 0 AND gr.product_offering LIKE '%Enterprise%' AND gr.product_line = 'Support', gr.baseplan_quantity, 0)) AS paid_support_enterprise_agents
      , MAX(IF(gr.net_arr > 0 AND gr.product_line IN ('Support'), ppm.plan_type, 0)) AS max_support_plan
      , ROUND(SAFE_DIVIDE(SUM(IF(gr.net_arr > 0 AND gr.product_line IN ('Support'), ppm.plan_type * gr.baseplan_quantity, 0))
                        , SUM(IF(gr.net_arr > 0 AND gr.product_line IN ('Support'), gr.baseplan_quantity, 0)))) AS avg_support_plan
    FROM `edw-prod-153420.edw_financials.qtd_mrr_granular_for_suite` AS gr
    JOIN `edw-prod-153420.edw_consolidated.product_dim_scd2` AS pd
      ON gr.productrateplancharge_id = pd.productrateplancharge_id 
      AND pd.dw_curr_ind = 'Y'
    LEFT JOIN `edw-prod-153420.product_usage_analyst_general.product_plan_matrix` AS ppm
      ON COALESCE(gr.base_product_plan, gr.product_plan) = ppm.product_plan
    WHERE gr.crm_account_id IS NOT NULL
      AND gr.product_offering NOT LIKE '%Talk Partner%'
      --AND gr.product_offering NOT LIKE '%Starter%'
      AND gr.net_arr > 0
    GROUP BY 1,2,3
),
  products as (
  	select
      mrr_date 
    , crm_account_id
    , STRING_AGG(DISTINCT product_line ORDER BY product_line) AS product_combo
    , SUM(base_product_count) AS base_product_count
    , SUM(all_product_count) AS all_product_count
    , IF(MAX(ela_flag) = 'Y', GREATEST(MAX(paid_agents), 1000), MAX(paid_agents)) AS paid_agents
    , IF(MAX(ela_flag) = 'Y', GREATEST(MAX(paid_support_agents), 1000), MAX(paid_support_agents)) AS paid_support_agents
    , IF(MAX(ela_flag) = 'Y', GREATEST(MAX(paid_support_enterprise_agents), 1000), MAX(paid_support_enterprise_agents)) AS paid_support_enterprise_agents
    , IF(MAX(ela_flag) = 'Y' OR STRING_AGG(DISTINCT offerings) LIKE '%Premier%', 5, MAX(max_support_plan)) AS max_support_plan
    , IF(MAX(ela_flag) = 'Y' OR STRING_AGG(DISTINCT offerings) LIKE '%Premier%', 5, MAX(avg_support_plan)) AS avg_support_plan
  from crm_product
 -- where agent_type = 'Admin'
  group by 1,2
 )

## saved under rw_mm_accounts_test
 select distinct 
  	a.crm_account_id,
  	a.the_date,
  	a.win_date,
  	a.churn_date,
 	  w.avg_support_plan as avg_support_plan_at_win,
    w.max_support_plan as max_support_plan_at_win,
    w.product_combo as product_combo_at_win,
    w.paid_agents as paid_agents_at_win,
    w.paid_support_agents as paid_support_agents_at_win,
    t.avg_support_plan as avg_support_plan,
    t.max_support_plan as max_support_plan,
    t.product_combo as product_combo,
    t.paid_agents,
    t.paid_support_agents
  from `edw-prod-153420.eda_analytics__prototype_views_gtm.customer_age` a 
	left join products w
    on a.crm_account_id = w.crm_account_id 
    AND GREATEST(a.win_date, '2014-01-01') = w.mrr_date
  left join products t
    ON a.crm_account_id = t.crm_account_id 
    AND least(a.the_date,COALESCE(date_sub(a.churn_date, INTERVAL 1 DAY),'9999-12-31')) = t.mrr_date
   where a.the_date = (select max(the_date) from `edw-prod-153420.eda_analytics__prototype_views_gtm.customer_age`)
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
),

arr as (
	select distinct mrr_date, crm_account_id, sum(net_arr_usd) as net_arr_usd
	from edw_financials.qtd_mrr_granular_for_suite
	where mrr_date = (select max(mrr_date) from edw_financials.qtd_mrr_granular_for_suite)
	group by 1,2
)

select distinct 
	c.mrr_date,
	-- Account Attributes 
	c.crm_account_id,
	a.win_year_mo,
	c.age_months,
	c.industry,
	c.crm_owner_region_clean,
	r2.vp_team__c as region_vp,
 	r2.dir_team__c as subregion_dir,
 	a.win_year_quarter,
	a.win_year,
	IF(c.crm_account_id in (select distinct crm_account_id from fraud_v2), "Fraudulent", "Not Fraudulent") AS fraud_flag,
	-- At Win Attributes 
	p.paid_agents_at_win,
	p.paid_support_agents_at_win,
	CASE WHEN p.paid_agents_at_win = 0 THEN '0'
           WHEN p.paid_agents_at_win = 1 THEN '1'
           WHEN p.paid_agents_at_win < 10 THEN '2 - 9'
           WHEN p.paid_agents_at_win < 50 THEN '10 - 49'
           WHEN p.paid_agents_at_win < 100 THEN '50 - 99'
           WHEN p.paid_agents_at_win < 500 THEN '100 - 499'
           WHEN p.paid_agents_at_win >= 500 THEN '500+'
    ELSE 'n/a' END AS agent_band_at_win,
    p.max_support_plan_at_win,
    p.product_combo_at_win,
    sales_model_at_win,
    term_at_win,
    product_mix_at_win,
  	IF(c.market_segment_at_win IS NULL, 'SMB', c.market_segment_at_win) AS market_segment_at_win,
  	crm_arr_band_ops_at_win,
  	crm_arr_sub_band_ops_at_win,
  	net_arr_usd_at_win,	
	-- Current Account Attributes 
p.paid_agents as paid_agents_today,
	p.paid_support_agents as paid_support_agents_today,
	CASE WHEN p.paid_agents = 0 THEN '0'
           WHEN p.paid_agents = 1 THEN '1'
           WHEN p.paid_agents < 10 THEN '2 - 9'
           WHEN p.paid_agents < 50 THEN '10 - 49'
           WHEN p.paid_agents < 100 THEN '50 - 99'
           WHEN p.paid_agents < 500 THEN '100 - 499'
           WHEN p.paid_agents >= 500 THEN '500+'
    ELSE 'n/a' END AS agent_band,
	p.max_support_plan,
	p.product_combo,
	sales_model,
	term,
	product_mix,
	IF(c.market_segment IS NULL, 'SMB', c.market_segment) AS market_segment,
	coalesce(sfdc.employee_range__c, sfdc.zd_num_employees__c) AS employee_size,
	IF(map.employee_range_band IS NULL, '1 - 99', map.employee_range_band) AS employee_range_band,
	crm_arr_band_ops,
	crm_arr_sub_band_ops,
	-- Engagement Metrics Today
	us.engaged_flag,
	us.num_engaged_products,
	us.core_products_activated,
	us.product_combo as core_product_combo,
	-- Account Actions/Changes from Win 
	max_plan_change_type,
	agent_change_type,
	paid_agents_change_from_win,
	term_change_from_win,
	product_mix_change_from_win,
	market_segment_change_from_win,
	crm_arr_band_ops_change_from_win,
	crm_arr_sub_band_ops_change_from_win,
	sales_model_change_from_win,
	case when arr.net_arr_usd > 0 then 'Customer' else 'Churned'
	end as active_status,
  arr.net_arr_usd
from `edw-prod-153420.eda_analytics__prototype_views_gtm.cohort` c 
left join arr on arr.crm_account_id = c.crm_account_id
--join edw_financials.qtd_mrr_granular_for_suite q on q.crm_account_id = c.crm_account_id and c.mrr_date = q.mrr_date 
left join `edw-prod-153420.eda_analytics__prototype_views_gtm.customer_age` a on a.crm_account_id = c.crm_account_id and a.the_date = c.mrr_date
left join `edw-prod-153420.sfdc.account_scd2` AS sfdc 
    on c.crm_account_id = sfdc.id 
    and TIMESTAMP(DATE_ADD(IF(c.mrr_date <= '2017-05-03', '2017-05-03', c.mrr_date), INTERVAL 1 DAY)) BETWEEN sfdc.dw_eff_start AND sfdc.dw_eff_end
 LEFT JOIN `edw-prod-153420.eda_analytics__prototype_views_gtm.employee_mapping` AS map
    ON sfdc.employee_range__c = map.employee_range
left join  `edw-prod-153420.sfdc.user_scd2` u on sfdc.ownerid = u.id 
	and TIMESTAMP(DATE_ADD(IF(c.mrr_date <= '2017-05-03', '2017-05-03', c.mrr_date), INTERVAL 1 DAY)) BETWEEN u.dw_eff_start AND u.dw_eff_end
left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	and TIMESTAMP(DATE_ADD(IF(c.mrr_date <= '2017-05-03', '2017-05-03', c.mrr_date), INTERVAL 1 DAY)) BETWEEN u2.dw_eff_start AND u2.dw_eff_end
left join `edw-prod-153420.sfdc.user_role_attribute__c` r2 on r2.role_label__c = u2.name     
	and TIMESTAMP(DATE_ADD(IF(c.mrr_date <= '2017-05-03', '2017-05-03', c.mrr_date), INTERVAL 1 DAY)) BETWEEN r2.dw_eff_start AND r2.dw_eff_end
left join usage us on us.crm_account_id = c.crm_account_id and us.run_at = c.mrr_date
left join product_combo p on p.crm_account_id = c.crm_account_id and p.the_date = c.mrr_date 
where c.crm_account_id IS NOT NULL
and c.crm_owner_region_clean is not null
    --and net_arr_usd > 0 
and day_type = 'max'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51

