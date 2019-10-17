select 
	crm_owner_region,
	product_mix_at_win,
	sales_model_at_win,
	term_at_win,
	market_segment,
	segment,
	active_status,
	first_expansion_type_grouped,
    case when months_to_first_expansion <= 12 then months_to_first_expansion
    else null
    end as months_to_first_expansion_grouped,
  	--	months_to_first_expansion,
  	"Expanded" as expansion_flag,
  	sum(first_expansion_arr) as first_exp_arr,
    count(distinct exp_account) as exp_accts
from marketing_analyst_general.rw_mm_q3_expansions 
where win_year >= 2016 
	and fraud_flag = 'Not Fraudulent'
	and product_mix not in ('Chat-Only', 'Starter-Only')
	--and first_expansion_type_grouped is not null
group by 1,2,3,4,5,6,7,8,9,10
