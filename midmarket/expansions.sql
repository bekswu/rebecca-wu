with first_expansion_by_type as (
	select 
		o2.crm_account_id,
		o2.opportunity_id,
		o2.yyyyqq_closed,
		o2.yyyymm_closed,
		case when o2.type_of_expansion in ('Agent Adds', 'Agent Add') then 'Agent Add'
		when o2.type_of_expansion in ('Add Product', 'New Product') then 'Add Product'
		when o2.type_of_expansion in ('Plan Shift', 'New Instance', 'New Contract Term') then o2.type_of_expansion
		else null 
		end as type_of_exp_grouped,
		o2.total_booking_arr as first_expansion_arr,
		min(o2.close_date) as first_expansion_date
	from (
		select 
			row_number() over (PARTITION BY accountid,type_of_expansion order by close_date asc) as row,
			accountid as crm_account_id,
			opportunity_id,
  			close_date,
  			yyyyqq_closed,
  			yyyymm_closed,
			type_of_expansion, 
			sum(total_booking_arr_usd) as total_booking_arr
		from `marketing_analyst_general.sfdc_opportunities_arr` 
		where stage_name = '07 - Closed'
			and type like '%Expansion%'
			and type_of_expansion in ('Agent Adds', 'Agent Add', 'Add Product', 'New Product', 'Plan Shift', 'New Instance', 'New Contract Term')
		group by 2,3,4,5,6,7
	) o2 
	where o2.row = 1 
	group by 1,2,3,4,5,6
),

exp_arr as (
	select distinct 
		e.crm_account_id,
		type_of_exp_grouped,
		sum(total_booking_arr) as total_exp_arr,
		count(distinct opportunity_id) as total_expansions
	from (
		select 
		accountid as crm_account_id,
		opportunity_id,
  		close_date,
  		yyyyqq_closed,
  		type_of_expansion,
  		case when type_of_expansion in ('Agent Adds', 'Agent Add') then 'Agent Add'
		when type_of_expansion in ('Add Product', 'New Product') then 'Add Product'
		when type_of_expansion in ('Plan Shift', 'New Instance', 'New Contract Term') then type_of_expansion
		else null 
		end as type_of_exp_grouped,
		sum(total_booking_arr_usd) as total_booking_arr
	from `edw-prod-153420.marketing_analyst_general.sfdc_opportunities_arr` o 
	where o.stage_name = '07 - Closed'
		and o.type like '%Expansion%'
		and type_of_expansion in ('Agent Adds', 'Agent Add', 'Add Product', 'New Product', 'Plan Shift', 'New Instance', 'New Contract Term')
	group by 1,2,3,4,5,6
	) e 
	group by 1,2
),


expansions as(	
	select 
		e.*,
		date_diff(e.first_expansion_date, e.win_date, day) as days_to_first_expansion,
		cast(floor(date_diff(e.first_expansion_date, e.win_date, day)/31)+1 as INT64) as months_to_first_expansion
	from (
		select distinct 
			w.crm_account_id,
			w.win_date,
			coalesce(f.type_of_exp_grouped, null) as first_expansion_type_grouped,
			coalesce(f.yyyymm_closed, null) as first_expansion_yyyymm,
			coalesce(f.yyyyqq_closed, null) as first_expansion_yyyyqq,
			coalesce(date(f.first_expansion_date), null) as first_expansion_date,
			coalesce(f.first_expansion_arr, 0) as first_expansion_arr,
			coalesce(arr.total_exp_arr,0) as total_exp_arr_by_type,
			coalesce(arr.total_expansions,0) as total_exp_by_type
		from marketing_analyst_general.rw_mm_q3_accounts_granular w 
		left join first_expansion_by_type f on f.crm_account_id = w.crm_account_id
		left join exp_arr as arr on arr.crm_account_id = f.crm_account_id and arr.type_of_exp_grouped = f.type_of_exp_grouped
	group by 1,2,3,4,5,6,7,8,9
	) e 
	where e.first_expansion_date > e.win_date 
),
  
 churn_dates as (
	select distinct 
		a.crm_account_id,
		a.churn_date,
		year_quarter as churn_year_quarter,
    	fiscal_year as churn_year,
    	a.win_date
	from `edw-prod-153420.eda_analytics__prototype_views_gtm.customer_age` a 
	join edw_consolidated.edw_date_dim d on a.churn_date = d.the_date 
  	join marketing_analyst_general.rw_mm_q3_accounts_granular q on q.crm_account_id = a.crm_account_id
  	where a.churn_date <= '2019-09-30'
	group by 1,2,3,4,5
) 
  
-- Predecessor table: marketing_analyst_general.rw_q3_mm_accounts_granular 
-- Destination table: marketing_analyst_general.rw_q3_mm_expansions  

select 
	w.crm_account_id,
	if(w.crm_account_id in (select distinct e.crm_account_id from expansions e), w.crm_account_id, NULL) as exp_account,
	if(w.crm_account_id in (select distinct c.crm_account_id from churn_dates c), w.crm_account_id, NULL) as churned_account,
	if(win_year_quarter in ('2018Q3', '2018Q4', '2019Q1', '2019Q2', '2019Q3'), "Post-Suite", "Pre-Suite") as segment,
	active_status,
	w.win_date,
  	win_year_quarter,
  	win_year_mo,
  	win_year,
  	c.churn_date,
	c.churn_year_quarter,
	c.churn_year,
	crm_owner_region_clean as crm_owner_region,
	agent_band_at_win,
	max_support_plan_at_win,
	product_combo_at_win,
  	sales_model_at_win,
  	term_at_win,
  	product_mix_at_win,
  	product_mix,
  	market_segment_at_win,
  	market_segment,
  	employee_size,
  	crm_arr_band_ops_at_win,
  	crm_arr_sub_band_ops_at_win,
  	product_mix_change_from_win,
  	market_segment_change_from_win,
	first_expansion_yyyymm,
	first_expansion_yyyyqq,
	first_expansion_date,
  	months_to_first_expansion,
  	days_to_first_expansion,
	first_expansion_type_grouped,
	first_expansion_arr,
	total_exp_arr_by_type,
  	total_exp_by_type,
	w.net_arr_change_from_win as arr_change_from_win,
	if(w.active_status = 'Churned', cast(floor(date_diff(c.churn_date, w.win_date, day)/31)+1 as INT64), cast(floor(date_diff(w.mrr_date, w.win_date, day)/31)+1 as INT64)) as age_months 
from marketing_analyst_general.rw_mm_q3_accounts_granular w 
left join expansions e on e.crm_account_id = w.crm_account_id 
left join churn_dates c on c.crm_account_id = w.crm_account_id
where fraud_flag = 'Not Fraudulent'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
order by crm_account_id 
