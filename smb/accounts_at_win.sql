-- Predecessor: marketing_analyst_general.rw_crm_accounts 
-- Destination table: marketing_analyst_general.rw_crm_accounts_at_win


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
)


	select distinct 
		c.crm_account_id,
		c.mrr_date,
		w.date as account_win_date,
		if(c.crm_account_id in (select distinct ca.crm_account_id from marketing_analyst_general.rw_crm_accounts ca), c.crm_account_id, NULL) AS active_account,
		if(c.crm_account_id not in (select distinct ca.crm_account_id from marketing_analyst_general.rw_crm_accounts ca), c.crm_account_id, NULL) AS churn_account,
		if(c.crm_account_id in (select distinct s.crm_account_id from marketing_analyst_general.rw_smb_startup_accounts s), c.crm_account_id, NULL) AS startup_account,
		c.crm_owner_region_clean as region,
		case when a.crm_business_unit is not null then a.crm_business_unit 
    	else "Unknown"
    	end as business_unit, 
    	case when a.sales_model is not null then a.sales_model
    	else "Unknown"
    	end as sales_model, 
    	IF(t.term >= 12, 'Annual', 'Monthly') as term,
		case when market_segment is not null then market_segment 
    	else "Unknown"
    	end as employee_segment_rollup,
    	coalesce(a2.employee_range__c, a2.zd_num_employees__c) as employee_size_granular,
    	coalesce(a2.industry, "Unknown") as industry,
    	coalesce(ca.customer_type, 'Churned') as customer_type_now,
		p.customer_type as customer_type_at_win,
		CONCAT(p.customer_type, ' to ', coalesce(ca.customer_type, 'Churned')) AS cust_type_win_to_now,
		ca.support_plan_now,
		coalesce(ca.support_plan_at_win, s2.support_plan_grouped, '') as support_plan_at_win,
		CONCAT(coalesce(ca.support_plan_at_win, s2.support_plan_grouped, ''), ' to ', coalesce(ca.support_plan_now, 'Churned')) AS support_plan_win_to_now,
		d.fiscal_year as win_year,
		d.month_year as win_yyyymm,
		d.year_quarter as win_yyyyqq,
		c.net_arr_usd as arr_at_win,
		case when net_arr_usd = 0 then 'n/a'
			 when net_arr_usd < 10000 then '1. <$10K'
			 when net_arr_usd < 100000 then '2. $10-100K'
			 when net_arr_usd >= 100000 then '3. $100K+'
		else 'Unknown'
		end as primary_arr_band_at_win,
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
		end as sub_arr_band_at_win
	from edw_financials.pop_mrr_crm c
	join marketing_analyst_general.rw_crm_win w on c.crm_account_id = w.crm_account_id and GREATEST(w.date, '2014-01-01') = c.mrr_date
	join edw_consolidated.edw_date_dim d on d.the_date = c.mrr_date
	left join marketing_analyst_general.rw_crm_accounts ca on ca.crm_account_id = w.crm_account_id --and ca.account_win_date = w.date 
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
  	left join accounts a on a.crm_account_id = c.crm_account_id 
  	left join product_mix p on p.crm_account_id = c.crm_account_id and p.mrr_date = c.mrr_date
  	--left join support_plan s on s.crm_account_id = c.crm_account_id and s.mrr_date = c.mrr_date
	left join support_plan s2 on s2.crm_account_id = c.crm_account_id and GREATEST(w.date, '2014-01-01') = s2.mrr_date 
  	left join `edw-prod-153420.sfdc.account_scd2` a2 on c.crm_account_id = a2.id
	  	and a2.dw_curr_ind = 'Y' 
	  	and a2.isdeleted = 'False'
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
  	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
