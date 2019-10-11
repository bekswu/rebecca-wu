-- Predecessor table: marketing_analyst_general.rw_mm_accounts_granular

with opps as (
	select distinct 
		id as lead_id, 
		mql,
		converted_accountid as crm_account_id,
		converted_opportunityid as opp_id,
  		date(created_date) as lead_created_date,
  		yyyymm_created  as lead_created_month,
  		yyyyqq_created as lead_created_quarter,
  		date(mql_date) as mql_date,
  		date(opportunity_created_date) as opp_created_date,
  		date(opportunity_close_date) as opp_close_date,
  		case when employee_range_at_signup_rollup in ('1 - 99','Unknown') then '1 - 99'
  		else employee_range_at_signup_rollup
  		end as employee_range_at_signup, 
    	case when derived_source_granular in ('Support Trial','Suite Trial') then 'Support/Suite Trial'
    		 when derived_source_granular in ('Contact Us', 'Demo Request') then 'Contact Us/Demo Request'
    	 	 when derived_source_granular = 'Chat Trial' then 'Chat Trial'
    		 else 'Partner/Outbound/Other'
    	end as derived_source_grouped,
    	derived_source_granular,
    	case when mql = 1 then 'MQL'
    	else null 
    	end as mql_flag,
      	lead_region,
    	date_diff(date(opportunity_close_date),date(created_date),day) as lead_to_win_days, 
    	date_diff(date(opportunity_close_date),date(mql_date),day) as mql_to_win_days ,
    	date_diff(date(opportunity_close_date),date(opportunity_created_date),day) as opp_to_win_days,
    	date_diff(date(opportunity_created_date),date(mql_date),day) as mql_to_opp_days,
  		sum(total_booking_arr_usd) as bookings
from `marketing_analyst_general.sfdc_leads_arr`
where type like ('%New%')
	and opportunity_stage_name = '07 - Closed'
  	and yyyyqq_created in ('2018Q3', '2018Q4', '2019Q1', '2019Q2') ##include Q3?
    --and date(opportunity_close_date) <= '2019-06-30'
	and derived_source_granular in ('Support Trial','Suite Trial', 'Contact Us', 'Demo Request', 'Chat Trial')
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

select 
	m.derived_source_granular,
	m.employee_range_at_signup,
	m.lead_region as lead_region,
	m.lead_created_quarter,
  	m.active_status,
	m.crm_owner_region_clean as crm_owner_region,
	m.agent_band_at_win,
	m.max_support_plan_at_win,
	m.product_combo_at_win,
  	m.sales_model_at_win,
  	m.term_at_win,
  	m.product_mix_at_win,
  	m.market_segment_at_win,
  	m.market_segment,
  	m.crm_arr_band_ops_at_win,
  	product_mix_change_from_win,
  	market_segment_change_from_win,
	count(distinct crm_account_id) as accounts, 
	sum(mql) as mqls,
	sum(net_arr_usd_at_win) as arr_at_win,
	sum(net_arr_usd) as arr_today,
  	sum(bookings) as new_biz_bookings
from (
	select 
		m.*, 
		crm_owner_region_clean,
    	active_status,
    	agent_band_at_win,
    	max_support_plan_at_win,
    	product_combo_at_win,
    	sales_model_at_win,
    	term_at_win,
    	product_mix_at_win,
    	market_segment_at_win,
    	market_segment,
    	crm_arr_band_ops_at_win,
    	net_arr_usd_at_win,
    	max_plan_change_type,
    	product_mix_change_from_win,
    	market_segment_change_from_win,
    	net_arr_usd
	from marketing_analyst_general.rw_mm_accounts_granular_2 s 
  	join opps m on s.crm_account_id = m.crm_account_id
	where fraud_flag = 'Not Fraudulent'
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
	) m
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17--,18
