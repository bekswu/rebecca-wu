with intervals as (
	select 
		c.crm_account_id,
		c.win_date,
    	c.win_year_quarter,
    	if(win_year_quarter in ('2018Q3', '2018Q4', '2019Q1', '2019Q2', '2019Q3'), "Post-Suite", "Pre-Suite") as segment,
		date_add(win_date, interval 1 month) as post_1mo,
		date_add(win_date, interval 2 month) as post_2mo,
		date_add(win_date, interval 3 month) as post_3mo,	
		date_add(win_date, interval 4 month) as post_4mo,
		date_add(win_date, interval 5 month) as post_5mo,
		date_add(win_date, interval 6 month) as post_6mo,
		date_add(win_date, interval 7 month) as post_7mo,
		date_add(win_date, interval 8 month) as post_8mo,
		date_add(win_date, interval 9 month) as post_9mo,
		date_add(win_date, interval 10 month) as post_10mo,
		date_add(win_date, interval 11 month) as post_11mo,
		date_add(win_date, interval 12 month) as post_12mo
	from marketing_analyst_general.rw_mm_q3_expansion c 
	where win_date >= '2016-01-01'
	and age_months >= 12
),

instances as (
	select distinct 
		sfdc_crm_id,
		d.account_id,
		d.subdomain,
		plan_name,
		plan_type,
		case when plan_name like '%Essential%' then 'Essential'
			when plan_name in ('Team', 'Regular') then 'Team'
			when plan_name in ('Professional', 'Plus') then 'Professional'
			when plan_name like '%Enterprise%' then 'Enterprise'
		else 'Other'
		end as support_plan_grouped,
		derived_account_type
	from pdw.derived_account_view d 
	join marketing_analyst_general.rw_mm_q3_expansion q on d.sfdc_crm_id = q.crm_account_id
	--join mrr m on d.account_id = m.account_id  
	where derived_account_type like '%Customer%'
	group by 1,2,3,4,5,6,7
)

select 
	m.crm_account_id,
	m.win_date,
	i.account_id,
	i.support_plan_grouped,
	i.derived_account_type,
	post_1mo,
	post_2mo,
	post_3mo,
	post_4mo,
	post_5mo,
	post_6mo,
	post_7mo,
	post_8mo,
	post_9mo,
	post_10mo,
	post_11mo,
	post_12mo

from marketing_analyst_general.rw_mm_q3_expansion m 
join instances i on i.sfdc_crm_id = m.crm_account_id
join intervals i2 on i2.crm_account_id = m.crm_account_id 
where post_12mo <= date_sub(current_date(), interval 1 day)
--and age_months >= 12
