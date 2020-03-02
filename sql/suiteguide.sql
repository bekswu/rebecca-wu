with date_to_use as (select max(mrr_date) as max from `edw_financials.qtd_mrr_granular_for_suite`), ##take max date for where clause below
   plan as ( select 
    	s2.*
    from (
    	select distinct  
       		s.mrr_date,  
       		s.crm_account_id, 
       		s.billing_id,
       		split(s.zendesk_account_id, ".")[offset(0)] as zendesk_account_id,
       		string_agg(distinct s.product_line) as product_mix, ## pulls in all paid products 
       		string_agg(distinct s.offerings) as product_plan, ## pulls in all paid product/plan combos 
       		MAX(paid_agents) AS paid_agents,
       		MAX(max_suite_plan) AS max_suite_plan, 
       		MAX(max_guide_plan) AS max_guide_plan
     	from (  
       		select  
           		gr.mrr_date,  
           		gr.crm_account_id, 
           		gr.account_id as billing_id, ## at the billing/instance level
           		cd.zendesk_account_id,
           		gr.product_line,  
           		STRING_AGG(DISTINCT IF(net_arr_usd > 0, gr.product_offering, null)) as offerings,  ## combines all product/plan offerings for each product 
           		MAX(IF(net_arr_usd > 0 AND gr.product_line = 'Suite', ppm.plan_type, 0)) AS max_suite_plan, ## join to product plan matrix to get equivalent plan types. 3 = Pro, 4 = Enterprise (classified below)
           		MAX(IF(net_arr_usd > 0 AND gr.product_line = 'Guide', ppm.plan_type, 0)) AS max_guide_plan, ## join to product plan matrix to get equivalent plan types. 3 = Pro, 4 = Enterprise (classified below)
           		SUM(IF(gr.net_arr > 0, gr.baseplan_quantity, 0)) AS paid_agents,
           		COUNT(DISTINCT CASE WHEN gr.product_offering LIKE '%Answer%' THEN 'Answer Bot' 
           		WHEN pd.product_type_derived = 'Addon' THEN 'Add-on'
           		ELSE gr.product_line END) AS all_product_count ## don't really need this but included just in case. not just base products.
       		from `edw_financials.qtd_mrr_granular_for_suite` as gr  
       		left join `edw-prod-153420.edw_consolidated.customer_dim_scd2` AS cd on gr.account_id = cd.billing_account_id 
    			and TIMESTAMP(DATE_ADD(IF(gr.mrr_date <= '2017-05-03', '2017-05-03', gr.mrr_date), INTERVAL 1 DAY)) BETWEEN cd.dw_eff_start AND cd.dw_eff_end
       		join `edw_consolidated.product_dim_scd2` as pd  
         		on gr.productrateplancharge_id = pd.productrateplancharge_id   
         		and pd.dw_curr_ind = "Y"  
       		left join `product_usage_analyst_general.product_plan_matrix` AS ppm  
         	on COALESCE(gr.base_product_plan, gr.product_plan) = ppm.product_plan  
      		where gr.crm_account_id IS NOT NULL  
         		and gr.net_arr_usd > 0 
       		group by 1,2,3,4,5
     ) s   
     group by 1,2,3,4
     ) s2
    where s2.mrr_date = (select max from date_to_use) ##latest snapshot 
    	--and (product_plan like '%Suite%' or product_plan like '%Guide Professional%' or product_plan like '%Guide Enterprise%') 

),



crm as (
	select 
		gr.mrr_date,
		gr.crm_account_id,
		gr.account_id as billing_id,
		cd.zendesk_account_id as zendesk_account_id,
		gr.crm_owner_region_clean,
		a.name as company,
		ur.name as crm_owner_role,
    r.region as crm_sales_region,
    coalesce(r.market_segment, e.market_segment, "SMB") as market_segment,
    	MIN(sales_model) AS sales_model,
    	IF(MAX(gr.currentterm) > 1, 'Annual+', 'Monthly') AS term,
    	CASE WHEN STRING_AGG(DISTINCT product_offering) = 'Support Starter' THEN 'Starter-Only'
           WHEN STRING_AGG(DISTINCT product_line) = 'Support' THEN 'Support-Only'
           WHEN STRING_AGG(DISTINCT product_line) = 'Chat' THEN 'Chat-Only'
           WHEN STRING_AGG(DISTINCT product_line) LIKE '%Suite%' THEN 'Suite'
           WHEN COUNT(DISTINCT product_line) > 1 THEN 'Other Multi-Product'
           ELSE 'Other'
        END AS product_mix,
        STRING_AGG(DISTINCT IF(net_arr_usd > 0, gr.currency, null)) as currency, ## either using this or using currency from customer_dim?
		  CASE WHEN SUM(net_arr_usd) = 0 THEN 'n/a'
           WHEN SUM(net_arr_usd) < 12000 THEN '$  1 - $12K'
           WHEN SUM(net_arr_usd) < 60000 THEN '$ 12K - $60K'
           WHEN SUM(net_arr_usd) < 120000 THEN '$ 60K - $120K'
           WHEN SUM(net_arr_usd) < 300000 THEN '$120K - $300K'
           WHEN SUM(net_arr_usd) >= 300000 THEN '$300K+'
         ELSE 'n/a' END AS crm_arr_band_ops,
        CASE WHEN SUM(net_arr_usd) = 0 THEN 'n/a'
           WHEN SUM(net_arr_usd) < 10000 THEN '<$10K'
           WHEN SUM(net_arr_usd) < 100000 THEN '$10-100K'
           WHEN SUM(net_arr_usd) >= 100000 THEN '$100K+'
         ELSE 'n/a' END AS crm_arr_band_primary,
         sum(net_arr_usd) as net_arr_usd 
	from `edw_financials.qtd_mrr_granular_for_suite` as gr  
	left join `edw-prod-153420.sfdc.account_scd2` AS a on gr.crm_account_id = a.id and a.dw_curr_ind = 'Y' and a.isdeleted = 'False'
  left join `edw-prod-153420.sfdc.user_scd2` u on u.id = a.ownerid and u.dw_curr_ind = "Y"                     
  left join `edw-prod-153420.sfdc.userrole_scd2` ur on ur.id = u.userroleid and ur.dw_curr_ind = "Y" 
  left join `marketing_analyst_general.csv_roles1` r on ur.name = r.userrole
  left join `marketing_analyst_general.csv_employeerange` e on a.employee_range__c = e.range
	left join `edw-prod-153420.edw_consolidated.customer_dim_scd2` AS cd on gr.account_id = cd.billing_account_id and cd.dw_curr_ind = 'Y'
	where gr.crm_account_id IS NOT NULL
    	and net_arr > 0 
      --and mrr_date = (select max(mrr_date) as max from `edw_financials.qtd_mrr_granular_for_suite`)
    group by 1,2,3,4,5,6,7,8,9
),

accounts as (
  select 
	age.the_date,
	age.crm_account_id,
	crm.billing_id,
	crm.zendesk_account_id,
	crm.company,
	-- Account Attributes 
	age.win_year_mo,
	age.win_date,
	age.suite_date AS suite_win_date,
  SUBSTR(CAST(age.suite_date AS STRING), 1, 7) AS suite_win_year_mo,
  age.age_months as account_tenure_months, 
  case when age_years < 1 THEN '0-1 yr'
         when age_years < 2 THEN '1-2 yr'
         when age_years < 3 THEN '2-3 yr'
         when age_years < 4 THEN '3-4 yr'
         when age_years < 5 THEN '4-5 yr'
         when age_years >= 5 THEN '5+ yrs'
    else 'Unknown' END AS age_range_band, 
    crm.sales_model AS sales_model,
    crm.term,
    crm.product_mix,
    crm.market_segment,
    crm.crm_owner_region_clean,
    crm.crm_arr_band_ops,
    crm.crm_arr_band_primary,
    -- Account owner info 
 	u.id as sfdc_account_owner_id,
 	u.name as sfdc_account_owner,
 	u2.name as sfdc_account_owner_role,
 	u.email as sfdc_account_owner_email,
 	coalesce(crm.net_arr_usd,0) as net_arr_usd
from `edw-prod-153420.eda_analytics__prototype_views_gtm.customer_age_view` AS age ## to pull in account tenure and suite win date information
join crm on age.crm_account_id = crm.crm_account_id
    and age.the_date = crm.mrr_date
left join `edw-prod-153420.sfdc.account_scd2` AS a on age.crm_account_id = a.id and a.dw_curr_ind = 'Y' and a.isdeleted = 'False'
left join  `edw-prod-153420.sfdc.user_scd2` u on a.ownerid = u.id 
	  and u.dw_curr_ind = 'Y' 
left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	  and u2.dw_curr_ind = 'Y'
where age.the_date >= '2014-01-01'
and age.the_date = (SELECT MAX(the_date) FROM `edw-prod-153420.eda_analytics__prototype_views_gtm.customer_age_view`)
),

-- Predecessor tables: marketing_analyst_general.rw_suiteguide_plan; marketing_analyst_general.rw_suiteguide_crm 
-- Destination table: rw_suiteguide_final 

accounts_settings as (
    select a.*
    from (
      select 
            RANK()OVER(PARTITION BY account_id ORDER BY run_at DESC) as row,
            account_id,
            num_facebook_pages,
            num_twitter_handles
    from `edw-prod-153420.pdw.accounts_settings` a
    ) a 
    where a.row = 1 
),

tickets as (
  select 
    t.account_id,
    sum(new_channel_facebook_msg + new_channel_facebook_post) as new_facebook_tix,
    sum(new_channel_twitter+new_channel_twitter_dm+new_channel_twitter_fave) as new_twitter_tix
  from pdw.tickets t 
  where date(t.run_at) >= date_sub(current_date(),interval 1 year)
  group by 1
),

social_sku as (
 select 
        s.*
    from (
        select 
            RANK()OVER(PARTITION BY crm_account_id ORDER BY mrr_date DESC) as row,
            crm_account_id,
            mrr_date,
            product_line,
            product_offering,
            net_arr_usd 
        from `edw_financials.qtd_mrr_granular_for_suite`
        where lower(product_offering) like '%social%'
        --and net_arr_usd > 0 
        group by 2,3,4,5,6
    ) s 
    where row = 1 
    and net_arr_usd > 0 
),

contact as (
  select distinct 
    a.zendeskaccountid__c as account_id,
    a.billtocontact_id, 
    c.personalemail, 
    c.workemail
  from `edw-prod-153420.zuora.account_scd2` a 
  left join `zuora.contact_scd2`  c on a.billtocontact_id = c.id 
  where a.dw_curr_ind = 'Y'
  group by 1,2,3,4
)

select distinct 
  s.crm_account_id,
  s.billing_id,
  split(s.zendesk_account_id, ".")[offset(0)] as zendesk_account_id,
  d.account_id,
  d.subdomain,
  company,
  win_year_mo,
  win_date,
  suite_win_date,
  suite_win_year_mo,
  account_tenure_months,
  age_range_band,
  s.sales_model,
  term,
  s.product_mix,
  market_segment,
  s.crm_owner_region_clean as crm_owner_region,
  cd.billing_country,
  cd.billing_currency,
  crm_arr_band_ops,
  crm_arr_band_primary,
  sfdc_account_owner_id
  sfdc_account_owner,
  sfdc_account_owner_role,
  sfdc_account_owner_email,
  concat(u.firstname, '', u.lastname) as success_owner, 
  u.email as success_owner_email,
  p.product_plan,
  case when p.max_suite_plan is not null and p.max_suite_plan = 4 then 'Suite Enterprise'
  when p.max_suite_plan is not null and p.max_suite_plan = 3 then 'Suite Professional'
  else 'N/A'
  end as max_suite_plan,
  case when p.max_guide_plan is not null and p.max_guide_plan = 4 then 'Guide Enterprise'
  when p.max_guide_plan is not null and p.max_guide_plan = 3 then 'Guide Professional'
  else 'N/A'
  end as max_guide_plan,
  p.paid_agents,
  d.derived_account_type,
  -- Agent Info
  IF(d.cust_owner_email=ae.agent_email,'Owner','Admin') AS agent_type,
  coalesce(split(ae.agent_name ," ")[OFFSET(0)],ae.agent_name) as first_name,
  coalesce(REGEXP_EXTRACT(ae.agent_name , r"\s(.*)"),ae.agent_name) as last_name,
  ae.agent_name AS agent_full_name,
  ae.agent_id AS agent_id,
  ae.agent_email as agent_email,
  t.name AS language,
  if(t2.new_facebook_tix > 0, 1, 0) as facebook_usage_account_flag, ## adding in social messaging sk 
  if(t2.new_twitter_tix > 0, 1, 0) as twitter_usage_account_flag,
  s2.num_twitter_handles,
  s2.num_facebook_pages,
  c.billtocontact_id as rev_ops_billto_contactid,
  c.workemail as revops_bill_to_email,
  if(s.crm_account_id in (select distinct sk.crm_account_id from social_sku sk), 1,0) social_sku
from accounts s 
left join plan p on p.crm_account_id = s.crm_account_id and s.zendesk_account_id = p.zendesk_account_id 
join pdw.derived_account_view d on d.sfdc_crm_id = s.crm_account_id and split(s.zendesk_account_id, ".")[offset(0)] = cast(d.account_id as string) and d.billing_id is not null
left join pdw.agent_email_addresses ae on ae.account_id = d.account_id
    and d.latest_run_at = ae.run_at
left join `pdw.translation_locales`  t on ae.locale_id = t.locale_id
left join edw_consolidated.customer_dim_scd2 cd on cd.zendesk_account_id = s.zendesk_account_id 
  and cd.dw_curr_ind = 'Y'
left join `edw-prod-153420.sfdc.account_scd2` AS a on s.crm_account_id = a.id and a.dw_curr_ind = 'Y' and a.isdeleted = 'False' 
left join  `edw-prod-153420.sfdc.user_scd2` u on a.success_owner__c = u.id and u.dw_curr_ind = 'Y' ## to get success owners 
left join tickets t2 on t2.account_id = d.account_id 
left join accounts_settings s2 on s2.account_id = d.account_id
left join contact c on split(c.account_id, ".")[offset(0)] = cast(d.account_id as string)
where ae.is_active = 1 
  and agent_type = 'Admin'
  and d.derived_account_type = 'Customer'
 and (product_plan like '%Suite%'
 or (product_plan not like '%Suite%'
  and product_plan like  '%Guide Professional%' or product_plan like '%Guide Enterprise%')
  or (product_plan not like '%Suite%' and s.crm_account_id not in  (select distinct k.crm_account_id from social_sku k)
  and (t2.new_facebook_tix > 0 or t2.new_twitter_tix > 0)))
  
