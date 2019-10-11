-- Predecessor table: marketing_analyst_general.rw_mm_expansions 
-- Destination table: marketing_analyst_general.rw_mm_expansion_1yr

with exp_dates as (
select 
		e.*, 
  		## adding in logic for day 30,90,120,X dates 
 		(case when current_date() >= date_add(account_win_date, interval 30 day) then date_add(account_win_date, interval 30 day) else null end) as date_30,
  		(case when current_date() >= date_add(account_win_date, interval 60 day) then date_add(account_win_date, interval 60 day) else null end) as date_60,
  		(case when current_date() >= date_add(account_win_date, interval 90 day) then date_add(account_win_date, interval 90 day) else null end) as date_90,
  		(case when current_date() >= date_add(account_win_date, interval 120 day) then date_add(account_win_date, interval 120 day) else null end) as date_120,
  		(case when current_date() >= date_add(account_win_date, interval 150 day) then date_add(account_win_date, interval 150 day) else null end) as date_150,
  		(case when current_date() >= date_add(account_win_date, interval 180 day) then date_add(account_win_date, interval 180 day) else null end) as date_180,
  		(case when current_date() >= date_add(account_win_date, interval 210 day) then date_add(account_win_date, interval 210 day) else null end) as date_210,
  		(case when current_date() >= date_add(account_win_date, interval 240 day) then date_add(account_win_date, interval 240 day) else null end) as date_240,
  		(case when current_date() >= date_add(account_win_date, interval 270 day) then date_add(account_win_date, interval 270 day) else null end) as date_270,
  		(case when current_date() >= date_add(account_win_date, interval 300 day) then date_add(account_win_date, interval 300 day) else null end) as date_300,
  		(case when current_date() >= date_add(account_win_date, interval 330 day) then date_add(account_win_date, interval 330 day) else null end) as date_330,
  		(case when current_date() >= date_add(account_win_date, interval 360 day) then date_add(account_win_date, interval 360 day) else null end) as date_360,
  		(case when current_date() >= date_add(account_win_date, interval 390 day) then date_add(account_win_date, interval 390 day) else null end) as date_390,
  		(case when current_date() >= date_add(account_win_date, interval 420 day) then date_add(account_win_date, interval 420 day) else null end) as date_420,
  		(case when current_date() >= date_add(account_win_date, interval 450 day) then date_add(account_win_date, interval 450 day) else null end) as date_450
	from marketing_analyst_general.rw_mm_expansions e
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
  ),
  
expansions as (	
  select distinct 
		e.crm_account_id,
		e.account_win_date,
    e.win_year_quarter,
    e.win_year,
    e.active_status,
    e.crm_owner_region,
    e.agent_band_at_win,
    e.max_support_plan_at_win,
    e.product_combo_at_win,
    e.sales_model_at_win,
    term_at_win,
  	product_mix_at_win,
  	market_segment_at_win,
  	market_segment,
		o.opportunity_id,
		o.close_date,
		o.type_of_expansion,
		-- Expansion ARR 
		sum(case when date_30 is not null and o.close_date <= date_30 then o.total_booking_arr else null end) as exp_arr_30d,
		sum(case when date_60 is not null and o.close_date > date_30 and o.close_date <= date_60 then o.total_booking_arr else null end) as exp_arr_60d,
		sum(case when date_90 is not null and o.close_date > date_60 and o.close_date <= date_90 then o.total_booking_arr else null end) as exp_arr_90d,
		sum(case when date_120 is not null and o.close_date > date_90 and o.close_date <= date_120 then o.total_booking_arr else null end) as exp_arr_120d,
		sum(case when date_150 is not null and o.close_date > date_120 and o.close_date <= date_150 then o.total_booking_arr else null end) as exp_arr_150d,
		sum(case when date_180 is not null and o.close_date > date_150 and o.close_date <= date_180 then o.total_booking_arr else null end) as exp_arr_180d,
		sum(case when date_210 is not null and o.close_date > date_180 and o.close_date <= date_210 then o.total_booking_arr else null end) as exp_arr_210d,
		sum(case when date_240 is not null and o.close_date > date_210 and o.close_date <= date_240 then o.total_booking_arr else null end) as exp_arr_240d,
		sum(case when date_270 is not null and o.close_date > date_240 and o.close_date <= date_270 then o.total_booking_arr else null end) as exp_arr_270d,
		sum(case when date_300 is not null and o.close_date > date_270 and o.close_date <= date_300 then o.total_booking_arr else null end) as exp_arr_300d,
		sum(case when date_330 is not null and o.close_date > date_300 and o.close_date <= date_330 then o.total_booking_arr else null end) as exp_arr_330d,
		sum(case when date_360 is not null and o.close_date > date_330 and o.close_date <= date_360 then o.total_booking_arr else null end) as exp_arr_360d,
		sum(case when date_390 is not null and o.close_date > date_360 and o.close_date <= date_390 then o.total_booking_arr else null end) as exp_arr_390d,
		sum(case when date_420 is not null and o.close_date > date_390 and o.close_date <= date_420 then o.total_booking_arr else null end) as exp_arr_420d,
		sum(case when date_450 is not null and o.close_date > date_420 and o.close_date <= date_450 then o.total_booking_arr else null end) as exp_arr_450d,
		-- Volume of Expansion Opportunities 
		sum(case when date_30 is not null and o.close_date <= date_30 then 1 else null end) as exp_30d,
		sum(case when date_60 is not null and o.close_date > date_30 and o.close_date <= date_60 then 1 else null end) as exp_60d,
		sum(case when date_90 is not null and o.close_date > date_60 and o.close_date <= date_90 then 1 else null end) as exp_90d,
		sum(case when date_120 is not null and o.close_date > date_90 and o.close_date <= date_120 then 1 else null end) as exp_120d,
		sum(case when date_150 is not null and o.close_date > date_120 and o.close_date <= date_150 then 1 else null end) as exp_150d,
		sum(case when date_180 is not null and o.close_date > date_150 and o.close_date <= date_180 then 1 else null end) as exp_180d,
		sum(case when date_210 is not null and o.close_date > date_180 and o.close_date <= date_210 then 1 else null end) as exp_210d,
		sum(case when date_240 is not null and o.close_date > date_210 and o.close_date <= date_240 then 1 else null end) as exp_240d,
		sum(case when date_270 is not null and o.close_date > date_240 and o.close_date <= date_270 then 1 else null end) as exp_270d,
		sum(case when date_300 is not null and o.close_date > date_270 and o.close_date <= date_300 then 1 else null end) as exp_300d,
		sum(case when date_330 is not null and o.close_date > date_300 and o.close_date <= date_330 then 1 else null end) as exp_330d,
		sum(case when date_360 is not null and o.close_date > date_330 and o.close_date <= date_360 then 1 else null end) as exp_360d,
		sum(case when date_390 is not null and o.close_date > date_360 and o.close_date <= date_390 then 1 else null end) as exp_390d,
		sum(case when date_420 is not null and o.close_date > date_390 and o.close_date <= date_420 then 1 else null end) as exp_420d,
		sum(case when date_450 is not null and o.close_date > date_420 and o.close_date <= date_450 then 1 else null end) as exp_450d
	from exp_dates e 
	left join (
		select distinct 
			o.accountid as crm_account_id,
			o.opportunity_id,
  			date(o.close_date) as close_date,
			o.type_of_expansion,
			sum(o.total_booking_arr_usd) as total_booking_arr
		from `edw-prod-153420.marketing_analyst_general.sfdc_opportunities_arr` o 
		where o.stage_name = '07 - Closed'
			and o.type like '%Expansion%'
  			and o.type_of_expansion not in ('Agent Contraction', 'Churn', 'Expiring One-Time Discount')
		group by 1,2,3,4 
	) o on o.crm_account_id = e.crm_account_id 
	where e.days_to_first_expansion > 0 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

select 
	win_year,
    active_status,
    crm_owner_region,
    max_support_plan_at_win,
    sales_model_at_win,
    term_at_win,
  	product_mix_at_win,
  	market_segment_at_win,
  	market_segment,
  	-- Exp ARR 
  	sum(exp_arr_30d) as exp_arr_30d,
    sum(exp_arr_60d) as exp_arr_60d,
    sum(exp_arr_90d) as exp_arr_90d,
    sum(exp_arr_120d) as exp_arr_120d,
    sum(exp_arr_150d) as exp_arr_150d,
    sum(exp_arr_180d) as exp_arr_180d,
    sum(exp_arr_210d) as exp_arr_210d,
    sum(exp_arr_240d) as exp_arr_240d,
    sum(exp_arr_270d) as exp_arr_270d,
    sum(exp_arr_300d) as exp_arr_300d,
    sum(exp_arr_330d) as exp_arr_330d,
    sum(exp_arr_360d) as exp_arr_360d,
    -- Exp Opps
    sum(exp_30d) as exp_30d,
    sum(exp_60d) as exp_60d,
    sum(exp_90d) as exp_90d,
    sum(exp_120d) as exp_120d,
    sum(exp_150d) as exp_150d,
    sum(exp_180d) as exp_180d,
    sum(exp_210d) as exp_210d,
    sum(exp_240d) as exp_240d,
    sum(exp_270d) as exp_270d,
    sum(exp_300d) as exp_300d,
    sum(exp_330d) as exp_330d,
    sum(exp_360d) as exp_360d
from expansions
group by 1,2,3,4,5,6,7,8,9
  
