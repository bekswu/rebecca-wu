select
	  customer_year,
	  first_month,
	  customer_date_key as cohort, 
	  product, 
	  plan,
	  count(distinct crm_id) as total_customers,
	  sum(case when churn_flag = 0 then 1 else 0 end) as total_returned,
	  sum(case when churn_flag = 1 then 1 else 0 end) as total_churned,
	  sum(case when months_to_churn = 0 or months_to_churn = 1 then 1 else 0 end) as churned_month_1,
	  sum(case when months_to_churn = 2 then 1 else 0 end) as churned_month_2,
	  sum(case when months_to_churn = 3 then 1 else 0 end) as churned_month_3,
	  sum(case when months_to_churn = 4 then 1 else 0 end) as churned_month_4,
	  sum(case when months_to_churn = 5 then 1 else 0 end) as churned_month_5,
	  sum(case when months_to_churn = 6 then 1 else 0 end) as churned_month_6,
	  sum(case when months_to_churn = 7 then 1 else 0 end) as churned_month_7,
	  sum(case when months_to_churn = 8 then 1 else 0 end) as churned_month_8,
	  sum(case when months_to_churn = 9 then 1 else 0 end) as churned_month_9,
	  sum(case when months_to_churn = 10 then 1 else 0 end) as churned_month_10,
	  sum(case when months_to_churn = 11 then 1 else 0 end) as churned_month_11,
	  sum(case when months_to_churn = 12 then 1 else 0 end) as churned_month_12
from ( 
	select
		  crm_id,
		  product,
		  plan,
		  customer_date, #date they become a customer 
		  churn_date, 
		  churn_flag, 
		  net_mrr,
		  concat(cast(extract(year from customer_date) as string),cast(extract(month from customer_date) as string)) as customer_date_key,
		  concat(cast(extract(year from churn_date) as string),cast(extract(month from churn_date) as string)) as churn_date_key,
		  extract(year from customer_date) as customer_year,
		  extract(month from customer_date) as first_month,
		  extract(year from churn_date) as churn_year,
		  extract(month from churn_date) as churn_month,
		  if(date_diff(churn_date, customer_date, month) < 0, 0, date_diff(churn_date, customer_date, month)) as months_to_churn
	from ( 
		select
			  alldates.crm_account_id as crm_id,
			  alldates.product as product,
			  alldates.plan as plan,
			  case when min(mindt) is not null then min(mindt) 
			  else null # returns null if customer has not churned
			  end as customer_date,
			  case when min(maxdt) is not null then min(maxdt) 
			  else null # returns null if customer has not churned
			  end as churn_date,
			  case when min(maxdt) is not null then 1 else 0
			  end as churn_flag,
			  sum(net_mrr_usd) as net_mrr,
			  sum(delta_mrr) as delta_mrr 
		from (
			select 
				-- at the CRM, Product, Plan level
    			c.crm_account_id as crm_account_id, 
    			p.product_line as product,
    			p.product_offering as plan,
    			p.mrr_date as mrr_date, 
    			p.net_mrr_usd as net_mrr_usd,
    			case when (COALESCE(p.net_mrr_usd, 0) = 0 and p.mtd_mrr_diff_type_transfer_adj in ('Product Offering Churn')) then 1 else 0 -- add in another case statement for contraction
    			end as mrr_churn, #indicates if customer has churned, based on if net_mrr_usd = 0; returns 0 or 1
      		case when (COALESCE(p.net_mrr_usd, 0) = 0 and p.mtd_mrr_diff_type_transfer_adj in ('Product Offering Churn')) then min(p.mrr_date) else null
      		end as maxdt, 
      		case when (COALESCE(p.net_mrr_usd, 0) > 0 and p.mtd_mrr_diff_type_transfer_adj in ('Product Offering New')) then min(p.mrr_date) else null
      		end as mindt, #earliest date a customer joined
        	c.crm_mrr_band,
        	c.crm_owner_region_clean,
        	p.mtd_mrr_diff_type_transfer_adj as type,
        	p.net_mrr_usd_mtd_diff_transfer_adj as delta_mrr 
  			from `edw_financials.mtd_mrr_crm` c
  			left join `edw_financials.mtd_mrr_product_offering` p on p.crm_account_id = c.crm_account_id and p.mrr_date = c.mrr_date 
  			where 1=1
  				and c.crm_account_id is not null
  				and c.crm_mrr_band like '%<1K%' #pulls in < $1k customers -- some of the mrr band calculations are wrong (not current) 
  				and (
  					product_offering like '%Starter%' 
  					or product_offering like '%Essential%' 
  					or product_offering like '%Starter%' 
  					or product_offering like '%Regular%' 
  					or product_offering like '%Team%' 
  					or product_offering like '%Plus%' 
  					or product_offering like '%Professional%' 
  					or product_offering like '%Enterprise%' 
  					or product_offering like '%Basic%' # confirm these with Jake
  					or product_offering like '%Advanced%'
  					or product_offering like '%Premium%'  
  					)
  					and c.crm_account_id is not null 
  					and p.product_line is not null 
  					and p.product_line NOT IN ('n/a')
  					and p.product_offering is not null 
  					--and lower(billingperiod) like '%month%' 
  				group by c.crm_account_id, product_line, product_offering, p.mrr_date, p.net_mrr_usd, mrr_churn, c.crm_mrr_band, c.crm_owner_region_clean, p.mtd_mrr_diff_type_transfer_adj, p.net_mrr_usd_mtd_diff_transfer_adj 
  			) as alldates 
		where 1=1
		group by crm_id, product, plan
	  ) alldates2
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) alldates3 
where customer_date_key is not null
group by 1,2,3,4,5
order by customer_year, first_month asc 
