-- Pulls in C/C at the CRM level 
select 
		c.mrr_date as mrr_date, 
		b.year_quarter as year_quarter,
		b.yyyymm as year_month,
		c.crm_owner_region_clean as region,
		sum(c.net_mrr_usd) as net_mrr, 
		sum(c2.churn_mrr) as churned_mrr,
		count(distinct c.crm_account_id) as total_customers,
		count(distinct c3.crm_account_id) as total_retained,
		count(distinct c2.crm_account_id) as total_churned,
		sum(low_chi) as low_chi,
		sum(high_chi) as high_chi 
	from `edw_financials.mtd_mrr_crm` c 
	inner join `edw_consolidated.edw_date_dim` b on (c.mrr_date = b.the_date 
 		and (b.last_day_of_mo_ind = 'Y')) # Pulls in the last day of the month. If you want to look at other date options, refer to Date Dim tab in the Data Dictionary.
	-- This subquery pulls in the current CHI score at the CRM level for < $1k customers. 
	left join (
		select
			crm_account_id,
			crm_mrr_band,
			health_score,
			date_scored,
			sum(case when health_score < 4 then 1 else 0 end) as low_chi, # low chi = 1-3
			sum(case when health_score >=4 then 1 else 0 end) as high_chi # high chi = 4 - 5
		from CHI_analyst_general.ml_chi_current_churn 
		where crm_mrr_band like '%<1K%'
		group by 1,2,3,4
	) m on m.crm_account_id = c.crm_account_id and m.date_scored = b.the_date 
	-- This subquery looks at churned customers and churned MRR 
	left join (
		select
			c.mrr_date as mrr_date,
      		c.crm_account_id as crm_account_id, # all of these CRM ID's represent churned customers
			c.crm_owner_region_clean as region,
			c.mtd_mrr_diff_type_transfer_adj as type,
			sum(c.net_mrr_usd_mtd_diff_transfer_adj) as churn_mrr 
		from `edw_financials.mtd_mrr_crm` c
		inner join `edw_consolidated.edw_date_dim` b on (c.mrr_date = b.the_date 
 			and (b.last_day_of_mo_ind = 'Y')) 
		where crm_mrr_band like '%<1K%'
			and c.mtd_mrr_diff_type_transfer_adj in ('Customer Contraction', 'Customer Churn') # to filter only on customer churn, delete the 'Customer Contraction' in the ().
      		and c.crm_owner_region_clean IS NOT NULL
		  	and c.crm_account_id IS NOT NULL
		  	and the_date >= '2015-03-31' # you can change this field to select certain date ranges or time periods. see example below.
		  	-- and the_date between '2017-01-01' and '2018-01-01'
		group by 1,2,3,4
	) c2 on c2.mrr_date = c.mrr_date and c2.crm_account_id = c.crm_account_id 
	-- This subquery looks at retained customers and retained mrr (ie. anything that does not fall under customer c/c)
	left join (
		select
			c.mrr_date as mrr_date, 
      		c.crm_account_id as crm_account_id,
			c.crm_owner_region_clean as region,
			c.mtd_mrr_diff_type_transfer_adj as type,
			sum(c.net_mrr_usd_mtd_diff_transfer_adj) as retained_mrr 
		from `edw_financials.mtd_mrr_crm` c
		inner join `edw_consolidated.edw_date_dim` b on (c.mrr_date = b.the_date 
 			and (b.last_day_of_mo_ind = 'Y')) 
		where crm_mrr_band like '%<1K%'
			and c.mtd_mrr_diff_type_transfer_adj not in ('Customer Contraction', 'Customer Churn') # to filter only on customer churn, delete the 'Customer Contraction' in the ().
      		and c.crm_owner_region_clean IS NOT NULL
		  	and c.crm_account_id IS NOT NULL
		  	and the_date >= '2015-03-31' # you can change this field to select certain date ranges or time periods. see example below.
		  	-- and the_date between '2017-01-01' and '2018-01-01'
		group by 1,2,3,4
	) c3 on c3.mrr_date = c.mrr_date and c3.crm_account_id = c.crm_account_id 
	where c.crm_mrr_band like '%<1K%'  
  		and c.crm_owner_region_clean IS NOT NULL # this field lets you filter by region. see example below
  		--and c.crm_owner_region_clean like '%AMER%' # the percent signs take away any anomolies that could occur before or after the region
  		--and c.crm_owner_region_clean in ('AMER', 'APAC') 
		and c.crm_account_id IS NOT NULL
		and the_date >= '2015-03-31' # you can change this field to select certain date ranges or time periods. see example below.
		--and the_date between '2017-01-01' and '2018-01-01'
	group by  1,2,3,4
	order by 1 desc
