-- Predecessor: marketing_analyst_general.rw_crm_win
-- Destination table: marketing_analyst_general.rw_crm_accounts 

with accounts as (
	select a.*
  	from (
    	select 
      		row_number() over (PARTITION BY cd.crm_account_id order by cd.date_extraction desc) as row,
      		cd.crm_account_id,
      		cd.date_extraction,
      		billing_account_type_clean,
      		crm_business_unit,
      		crm_owner_region_clean,
      		min(sales_model) as sales_model
    	from `edw_consolidated.customer_dim_scd2` cd 
    	where cd.dw_curr_ind = "Y" 
      		and billing_account_type_clean not in ("Sponsored", "Test", "Employee","Fraudulent") 
      		and crm_account_id is not null
    	group by 2,3,4,5,6
  	) a 
  	where a.row = 1 
),


product_mix as (
	select 
		mrr_date,
		crm_account_id,
		max(currentterm) as term,
		case when string_agg(distinct g.product_offering) = 'Support Starter' then 'Starter-only'
           	 when string_agg(distinct g.product_line) like '%Suite%' then 'Suite'
           	 when string_agg(distinct g.product_line) = 'Support' then 'Support-only'
           	 when string_agg(distinct g.product_line) = 'Chat' then 'Chat-only'
           	 when count(distinct g.product_line) > 1 then 'Other Multi-Product'
           	 else 'Other'
    	end as customer_type
	from `edw_financials.qtd_mrr_granular_for_suite` g 
	where crm_account_id is not null 
    	and net_arr_usd > 0 
    group by 1,2
),


support_plan as (
	select 
		s.*except(term,product_plan, support_plan_grouped),
		STRING_AGG(DISTINCT support_plan_grouped) as support_plan_grouped 
	from (
		select 
			mrr_date,
			crm_account_id,
			max(currentterm) as term,
			product_line,
			product_plan,
			case when product_plan in ('Essential', 'Starter') then 'Support Essential'
			 	when product_plan in ('Team', 'Regular') then 'Support Team'
			 	when product_plan in ('Professional', 'Plus', 'Premium') then 'Support Professional'
			 	when product_plan like '%Enterprise%' then 'Support Enterprise'
			else 'Other'
			end as support_plan_grouped 
		from `edw_financials.qtd_mrr_granular_for_suite` g 
		where crm_account_id is not null 
    		and net_arr_usd > 0 
    		and product_line like '%Support%'
    	group by 1,2,4,5,6
    ) s 
    group by 1,2,3

),


agents as (
	select 
		a.mrr_date,
		a.crm_account_id,
		IF(MAX(ela_flag) = 'Y', GREATEST(MAX(paid_core_agents), 1000), MAX(paid_core_agents)) as paid_core_agents
	from (
		select 
			g.mrr_date,
			g.crm_account_id,
			g.product_line,
			MAX(p.enterpriselicense_ind) as ela_flag,
			SUM(IF(g.net_arr > 0 AND g.product_line IN ('Support','Chat','Talk'), g.baseplan_quantity, 0)) as paid_core_agents
		from `edw_financials.pop_mrr_granular` g
    	join `edw_consolidated.product_dim_scd2` p on g.productrateplancharge_id = p.productrateplancharge_id 
      		and p.dw_curr_ind = 'Y'
    	where g.crm_account_id IS NOT NULL
      		and g.product_offering NOT LIKE '%Talk Partner%'
      		and g.product_offering NOT LIKE '%Starter%'
      		and g.net_arr > 0
    	group by 1,2,3
	) a 
	group by 1,2
),

crm_accounts as (
 select distinct 
    	c.crm_account_id,
    	if(c.crm_account_id in (select distinct s.crm_account_id from marketing_analyst_general.rw_crm_startups s), c.crm_account_id, NULL) AS startup_account,
    	c.mrr_date as mrr_date,
    	c.crm_owner_region_clean as region, 
    	--r.vp_team__c as region_vp, 
 	--r.dir_team__c as subregion, 
    	case when a.crm_business_unit is not null then a.crm_business_unit 
    	else "Unknown"
    	end as business_unit,
    	case when a.sales_model is not null then a.sales_model
    	else "Unknown"
    	end as sales_model, 
    	IF(t.term >= 12, 'Annual', 'Monthly') as term,
    	c.crm_arr_band_primary as crm_arr_band_primary,
    	c.crm_arr_band_secondary as crm_arr_band_secondary,
	case when net_arr_usd = 0 then 'n/a'
	when net_arr_usd < 1000 then 'A. $0-1K'
	when net_arr_usd < 2000 then 'B. $1-2K'
	when net_arr_usd < 3000 then 'C. $2-3K'
	when net_arr_usd < 4000 then 'D. $3-4K'
	when net_arr_usd < 5000 then 'E. $4-5K'
	when net_arr_usd < 6000 then 'F. $5-6K'
	when net_arr_usd < 7000 then 'G. $6-7K'
	when net_arr_usd < 8000 then 'H. $7-8K'
	when net_arr_usd < 9000 then 'I. $8-9K'
	when net_arr_usd < 10000 then 'J. $9-10K'
	when net_arr_usd < 11000 then 'K. $10-11K'
	when net_arr_usd < 12000 then 'L. $11-12K'
	when net_arr_usd < 24000 then 'M. $12-24K'
	when net_arr_usd < 36000 then 'N. $24-36K'
	when net_arr_usd < 48000 then 'O. $36-48K'
	when net_arr_usd < 60000 then 'P. $48-60K'
	when net_arr_usd < 120000 then 'Q. $60-120K'
	when net_arr_usd < 300000 then 'R. $120-300K'
	when net_arr_usd >= 300000 then 'S. $300K+'
	else 'Unknown'
	end as sub_arr_band,
    	case when market_segment is not null then market_segment 
    	else "Unknown"
    	end as employee_segment_rollup,
    	coalesce(a2.employee_range__c, a2.zd_num_employees__c) as employee_size_granular,
    	coalesce(a2.industry, "Unknown") as industry,
    	p.customer_type as customer_type,
    	aa.paid_core_agents as paid_agents_today,
    	(c.net_arr_usd) as net_arr, 
    	(c.net_arr_usd_prior_qtr_end) as prior_arr,
    	w.crm_win_date as account_win_date, 
		cast(floor(date_diff(current_date(), w.crm_win_date, day)/31)+1 as INT64) as age_months,
		date_diff(current_date(), w.crm_win_date, day) as age_days
  	from `edw_financials.pop_mrr_crm` c 
  	join edw_consolidated.edw_date_dim d on d.the_date = c.mrr_date 
  	join marketing_analyst_general.rw_crm_win w on w.crm_account_id = c.crm_account_id  
  	left join accounts a on a.crm_account_id = c.crm_account_id 
  	left join product_mix p on p.crm_account_id = c.crm_account_id and p.mrr_date = c.mrr_date
  	left join support_plan s on s.crm_account_id = c.crm_account_id and s.mrr_date = c.mrr_date
  	left join agents aa on aa.crm_account_id = c.crm_account_id and c.mrr_date = aa.mrr_date 
  	left join (
    	select distinct
        	sfdc.id AS crm_account_id,
        	map.employee_range_band,
        	if(map.market_segment IS NULL, 'SMB', map.market_segment) AS market_segment
    	from `sfdc.account_scd2` as sfdc
    	left join `eda_analytics__prototype_views_gtm.employee_mapping` as map on sfdc.employee_range__c = map.employee_range 
    	where sfdc.dw_curr_ind = 'Y' 
    	group by 1,2,3
  	) sfdc on sfdc.crm_account_id = c.crm_account_id 
  	left join `edw-prod-153420.sfdc.account_scd2` a2 on c.crm_account_id = a2.id
	  	and a2.dw_curr_ind = 'Y' 
	  	and a2.isdeleted = 'False'
	/*left join `edw-prod-153420.sfdc.user_scd2` u on a2.ownerid = u.id 
	  	and u.dw_curr_ind = 'Y' 
	left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	  	and u2.dw_curr_ind = 'Y'
  	left join `edw-prod-153420.sfdc.user_role_attribute__c` r on r.role_label__c = u2.name     
    	and r.dw_curr_ind = "Y" 
    	*/
  	left join (
      select  
          crm_account_id, 
          mrr_date, 
          max(term) AS term
      from (
          select 
            crm_account_id, 
            mrr_date, 
            COALESCE(currentterm, currentterm_prior_qtr_end) as term, 
            sum(net_mrr_usd) AS net_mrr_usd,
            RANK() OVER (PARTITION BY crm_account_id, mrr_date order by sum(net_mrr_usd) desc) as row
          from `edw-prod-153420.edw_financials.qtd_mrr_granular_for_suite` a
          inner join `edw-prod-153420.edw_consolidated.edw_date_dim` b on (a.mrr_date = b.the_date 
            and (b.last_day_of_qtr_ind = 'Y' or b.the_date = date_sub(current_date("America/Los_Angeles"), interval 1 day)))
            and b.fiscal_year >= 2015 
            and crm_account_id is not null 
        group by 1,2,3
        )
      where row = 1
      group by 1,2
  	) t on c.mrr_date = t.mrr_date and c.crm_account_id = t.crm_account_id 
  	where c.mrr_date = '2019-03-31' --(select max(c2.mrr_date) from `edw_financials.pop_mrr_crm` c2)
    	and c.crm_owner_region_clean is not null 
    	and c.crm_account_id is not null 
    	and c.net_arr_usd > 0 
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)

select 
	c.*,
	pw.customer_type as customer_type_at_win,
	concat(pw.customer_type, ' to ', c.customer_type) AS cust_type_win_to_now,
	coalesce(s.support_plan_grouped, '') as support_plan_now,
	coalesce(s2.support_plan_grouped, '') as support_plan_at_win,
	concat(s2.support_plan_grouped, ' to ', s.support_plan_grouped) AS support_plan_win_to_now,
	aa.paid_core_agents as paid_core_agents_at_win,
	(paid_agents_today - paid_core_agents) as agent_diff,
	case when aa.paid_core_agents = 0 then SAFE_DIVIDE((paid_agents_today-paid_core_agents),1)*100.0
      	 else SAFE_DIVIDE((paid_agents_today-paid_core_agents),paid_core_agents)*100.0
    	end as agent_change_pct 
from marketing_analyst_general.rw_crm_accounts c 
left join product_mix pw on pw.crm_account_id = c.crm_account_id and GREATEST(c.account_win_date, '2014-01-01') = pw.mrr_date
left join support_plan s on s.crm_account_id = c.crm_account_id and s.mrr_date = c.mrr_date
left join support_plan s2 on s2.crm_account_id = c.crm_account_id and GREATEST(c.account_win_date, '2014-01-01') = s2.mrr_date
left join agents aa on aa.crm_account_id = c.crm_account_id and GREATEST(c.account_win_date, '2014-01-01') = aa.mrr_date 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
