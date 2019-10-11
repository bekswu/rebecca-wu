-- Predecessor: marketing_analyst_general.rw_crm_at_win
-- Destination table: marketing_analyst_general.rw_crm_expansions 


with first_expansion as (
	select 
		o2.crm_account_id,
		o2.opportunity_id,
		o2.yyyyqq_closed,
		o2.yyyymm_closed,
		min(o2.close_date) as first_expansion_date,
		o2.type_of_expansion as first_expansion_type,
		o2.total_booking_arr as first_expansion_arr
	from `marketing_analyst_general.sfdc_opportunities_arr` o
	left join (
		select 
			row_number() over (PARTITION BY accountid order by close_date asc) as row,
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
			and type_of_expansion not in ('Agent Contraction', 'Churn', 'Expiring One-Time Discount')
		group by 2,3,4,5,6,7
	) o2 on o2.crm_account_id = o.accountid and o2.row = 1 
	group by 1,2,3,4,6,7
),


last_expansion as (
	select 
		o2.crm_account_id,
		o2.opportunity_id,
		o2.yyyyqq_closed,
		o2.yyyymm_closed,
		max(o2.close_date) as last_expansion_date,
		o2.type_of_expansion as last_expansion_type,
		o2.total_booking_arr as last_expansion_arr
	from `marketing_analyst_general.sfdc_opportunities_arr` o
	left join (
		select 
			row_number() over (PARTITION BY accountid order by close_date desc) as row,
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
			and type_of_expansion not in ('Agent Contraction', 'Churn', 'Expiring One-Time Discount')
		group by 2,3,4,5,6,7
	) o2 on o2.crm_account_id = o.accountid and o2.row = 1 
	group by 1,2,3,4,6,7
),

-- under rw_expansions
expansions as (
	select 
		e.*
	from (
		select distinct 
			w.crm_account_id,
			w.account_win_date,
			coalesce(f.yyyymm_closed, null) as first_expansion_yyyymm,
			coalesce(f.yyyyqq_closed, null) as first_expansion_yyyyqq,
			coalesce(date(f.first_expansion_date), null) as first_expansion_date,
			coalesce(f.first_expansion_type, null) as first_expansion_type,
			coalesce(f.first_expansion_arr, 0) as first_expansion_arr,
			coalesce(date(l.last_expansion_date), null) as last_expansion_date,
			coalesce(l.last_expansion_type, null) as last_expansion_type,
			coalesce(l.last_expansion_arr, 0) as last_expansion_arr,
			count(distinct o.opportunity_id) as num_expansions,
			string_agg(distinct o.type_of_expansion order by o.type_of_expansion asc) as types_of_exp,
			case when string_agg(distinct o.type_of_expansion order by o.type_of_expansion asc) in ('Agent Adds', 'Agent Adds,Temporary Agents', 'Temporary Agents') then 'Agent Expansion Only'
         	 	 when string_agg(distinct o.type_of_expansion) = 'Plan Shift' then 'Plan Shift Only'
         	 	 when string_agg(distinct o.type_of_expansion order by o.type_of_expansion asc) in ('Add Product', 'Add Product,New Product', 'New Product') then 'Product Expansion Only'
         	 	 when string_agg(distinct o.type_of_expansion) = 'New Instance' then 'New Instance Only'
        	 	 when string_agg(distinct o.type_of_expansion) = 'New Contract Term' then 'New Contract Only'
        	 	 when count(distinct o.type_of_expansion) > 1 then 'Multiple Expansions'
        	else 'Other'
    		end as account_exp_type,
    		count(distinct case when o.type_of_expansion in ('Agent Adds', 'Temporary Agents') then o.opportunity_id else null end) as total_agent_expansions,
    		count(distinct case when o.type_of_expansion = 'Plan Shift' then o.opportunity_id else null end) as total_plan_shifts,
    		count(distinct case when o.type_of_expansion in ('Add Product', 'New Product') then o.opportunity_id else null end) as total_product_expansions,
    		count(distinct case when o.type_of_expansion = 'New Instance' then o.opportunity_id else null end) as total_new_instance_expansions,
    		count(distinct case when o.type_of_expansion = 'New Contract Term' then o.opportunity_id else null end) as total_new_contract_expansions,
    		sum(case when o.type_of_expansion in ('Agent Adds', 'Temporary Agents') then o.total_booking_arr_usd else 0 end) as agent_exp_arr,
    		sum(case when o.type_of_expansion = 'Plan Shift' then o.total_booking_arr_usd else 0 end) as plan_shift_exp_arr,
    		sum(case when o.type_of_expansion in ('Add Product', 'New Product') then o.total_booking_arr_usd else 0 end) as product_exp_arr,
    		sum(case when o.type_of_expansion = 'New Instance' then o.total_booking_arr_usd else 0 end) as new_instance_exp_arr,
    		sum(case when o.type_of_expansion = 'New Contract Term' then o.total_booking_arr_usd else 0 end) as new_contract_exp_arr,   
			sum(o.total_booking_arr_usd) as total_exp_arr
		from `edw-prod-153420.marketing_analyst_general.sfdc_opportunities_arr` o 
		join marketing_analyst_general.rw_crm_at_win w on w.crm_account_id = o.accountid 
		left join first_expansion f on f.crm_account_id = o.accountid 
		left join last_expansion l on l.crm_account_id = o.accountid 
		where o.stage_name = '07 - Closed'
			and o.type like '%Expansion%'
			and o.type_of_expansion not in ('Agent Contraction', 'Churn', 'Expiring One-Time Discount')
	group by 1,2,3,4,5,6,7,8,9,10
	) e 
	where e.first_expansion_date > e.account_win_date
)

select 
	w.*,
	if(w.crm_account_id in (select distinct e.crm_account_id from marketing_analyst_general.rw_crm_expansion_accounts e), w.crm_account_id, NULL) as exp_account,
	if(w.crm_account_id in (select distinct e.crm_account_id from marketing_analyst_general.rw_crm_expansion_accounts e), date_diff(e.first_expansion_date, e.account_win_date, day), NULL) as days_to_first_expansion,
	if(w.crm_account_id in (select distinct e.crm_account_id from marketing_analyst_general.rw_crm_expansion_accounts e), cast(floor(date_diff(e.first_expansion_date, e.account_win_date, day)/31)+1 as INT64), NULL) as months_to_first_expansion,
	first_expansion_yyyymm,
	first_expansion_yyyyqq,
	first_expansion_date,
	first_expansion_type,
	first_expansion_arr,
	last_expansion_date,
	last_expansion_type,
	last_expansion_arr,
	num_expansions,
	types_of_exp,
	account_exp_type,
	total_agent_expansions,
	total_plan_shifts,
	total_product_expansions,
	total_new_instance_expansions,
	total_new_contract_expansions,
	agent_exp_arr,
	plan_shift_exp_arr,
	product_exp_arr,
	new_instance_exp_arr,
	new_contract_exp_arr,
	total_exp_arr
from marketing_analyst_general.rw_crm_at_win w 
left join expansions e on e.crm_account_id = w.crm_account_id 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47

