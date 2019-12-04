
with accounts as (
	select distinct 
		account_id,
		sfdc_crm_id,
		derived_account_type,
		latest_run_at,
		cust_owner_email,
		owner_id as agent_owner_id,
		subdomain,
		region,
		country,
		industry_full_name,
		r.subdomain AS support_subdomain,
		r.region AS support_account_region,
		r.country AS support_account_country,
		r.industry_full_name AS support_account_industry,
		r.is_sandbox AS support_account_sandbox,
		r.derived_account_type AS support_derived_account_type,
		r.is_active AS support_active_flag,
		r.is_trial AS support_active_trial_flag,
		r.trial_expired AS support_trial_expired_flag,
		r.trial_expires_on AS support_trial_expires_on,
		r.created_at AS support_instance_created_date,
		r.plan_name AS support_plan_name,
		r.max_agents AS support_max_agents,
		r.employee_count AS support_account_employee_count,
		r.help_desk_size AS support_help_desk_size,
		r.churn_dt as support_churn_date,
		r.is_churned AS support_churned_flag,
		r.win_dt AS support_win_date,
		r.risk_score AS support_risk_score,
		If(r.is_trial = 1, DATE_DIFF(DATE(r.trial_expires_on),CURRENT_DATE(), DAY ), null) as support_days_left_in_trial
	from pdw.derived_account_view r 
	where (derived_account_type in ('Trial','Customer') or (derived_account_type = 'Trial - expired' and trial_expires_on >= '2019-01-01'))
		and account_id is not null 
	--group by 1,2,3,4
),

agent as (
	select 
		ae.account_id,
		ae.run_at,
		CONCAT(IFNULL(CAST(ae.account_id AS string),
		''),IFNULL(CAST (ae.agent_email AS string),
		'')) AS unique_identifier,
		ae.agent_email,
		ae.agent_id as agent_id, 
		ae.agent_name,
		IF(ae.agent_email = a.cust_owner_email,'Owner','Admin') AS derived_agent_type, 
		t.locale AS agent_language,
		ae.is_active AS agent_is_active,
		ae.is_verified AS agent_is_verified,
		ae.agent_created_at as agent_created_date
	from `edw-prod-153420.pdw.agent_email_addresses` ae
	join accounts a on a.account_id = ae.account_id and a.latest_run_at = ae.run_at
	left join `edw-prod-153420.pdw.translation_locales`  t on ae.locale_id = t.locale_id
	where ae.agent_type = 'Admin'
	 	and ae.is_active = 1 
	group by 1,2,3,4,5,6,7,8,9,10,11
),

agent_info as ( -- pull in num_agents, num_admins, num_light_agents 
	select 
		r.account_id, 
		num_users,
		num_admins, 
		num_agents,
		num_agents/num_admins as agent_to_admin_ratio,
		num_light_agents
	from `edw-prod-153420.pdw.roles` r 
	join accounts a on a.account_id = r.account_id 
	where run_at = (select max(run_at) from `edw-prod-153420.pdw.roles`)
),

answer_bot as (
	select 
		ab.account_id,
		--IF(r.account_id =ab.account_id,'1','0') AS answer_bot_product_flag,
		ab.created_at AS answer_bot_instance_created_date,
		ab.trial_expires_at AS answer_bot_trial_expires_date,
		ab.days_elapsed AS answer_bot_days_elapsed,
		ab.estimated_resolutions AS answer_bot_estimated_resolutions,
		ab.max_resolutions AS answer_bot_max_resolutions,
		ab.product_state AS answer_bot_product_state,
		ab.resolutions_used AS answer_bot_resolutions_used,
		ab.boosted_resolutions AS answer_bot_boosted_resolutions,
		ab.boost_expires_at AS answer_bot_boost_expires_at
	from `edw-prod-153420.pdw.answer_bot_accounts`  ab
	join accounts a on a.account_id = ab.account_id and a.latest_run_at = ab.run_at
),

explore_accounts as (
	select 
		e.account_id,
		e.state AS explore_account_type,
		e.plan_name AS explore_plan_name, -- updated field from plan_code to plan name in Q2
		e.is_initialized AS explore_activation_flag,
		e.activated_at AS explore_activation_date
	from `edw-prod-153420.pdw.explore_accounts_soc2`  e
	join accounts a on a.account_id = e.account_id and a.latest_run_at = e.run_at
),


combo_metrics as (
	select
		a.account_id,
		zacct.No_of_Calls_in_last_30_days__c,
		zacct.IVR_Enabled__c,
		zacct.Support_Agents__c AS support_agents,
		zacct.Support_Email_Connector_Enabled__c AS support_email_connector_enabled,
		zacct.Support_Tickets__c AS support_tickets,
		zacct.Support_One_Touch_Tickets__c AS support_one_touch_tickets,
		zacct.Support_Custom_Ticket_Fields__c AS support_custom_ticket_fields,
		zacct.Tickets_Created_Last_14_Days__c as support_tickets_created_last_14_days, 
		zacct.Tickets_Created_Last_30_Days__c as support_tickets_created_last_30_days, 
		zacct.chat_enabled_date__c,
		zacct.cti_partner_plan__c AS talk_cti_partner_plan,
		zacct.Telephony_Credits_Remaining__c AS talk_telephony_Credits_Remaining,
		zacct.Talk_Last_Known_Plan__c AS talk_last_known_plan,
		zacct.Talk_Initial_Channel__c AS talk_initial_channel,
		zacct.SMS_Enabled__c AS talk_sms_enabled,
		zacct.Talk_No_of_Agents__c AS talk_total_agents,
		zacct.zd_last_login__c as support_last_login
	from `edw-prod-153420.pdw.daily_combo_push_to_sfdc` zacct 
	join accounts a on a.account_id = CAST(SAFE_CAST(zacct.zd_id__c as FLOAT64) AS INT64)
),

instances as (
	select 
		zendesk_account_id,
		voltron_id,
		zopim_number,
		support_win_dt,
		support_churn_dt,
		chat_win_dt,
		chat_churn_dt,
		guide_win_dt,
		guide_churn_dt,
		talk_win_dt,
		talk_churn_dt,
		explore_win_dt
		explore_churn_dt
	from pdw.instances_by_win_and_churn_dates i 
	join accounts a on i.zendesk_account_id = a.account_id 
),

chat_metrics as (
	select 
		i.zendesk_account_id,
		i.voltron_id,
		i.zopim_number,
		coalesce(v2.conversations, c2.conversations) as chat_conversations_in_last_day
	from instances i 
	left join (
		select v.*
		from (
			select 
				v.account_id,
				conversations, 
				RANK()OVER(PARTITION BY v.account_id ORDER BY timestamp DESC) as row
			from  `edw-prod-153420.chat_data.voltron_account_stats` v
			join instances i on i.voltron_id = v.account_id
		) v
		where v.row = 1 
	) v2 on v2.account_id = i.voltron_id 
	left join (
		select c.*
		from (
			select 
				c.account_id,
				conversations, 
				RANK()OVER(PARTITION BY c.account_id ORDER BY timestamp DESC) as row
			from  `edw-prod-153420.chat_data.account_stats` c
			join instances i on i.zopim_number = c.account_id
		) c
		where c.row = 1 
	) c2 on i.zopim_number = c2.account_id
	where (voltron_id is not null or zopim_number is not null)
	group by 1,2,3,4
),

shift as (
	select 
		a.account_id,
		s.model_plan as plan_shift_model_plan, -- New Field in Q3 
		s.probability as plan_shift_probability, -- New Field in Q3
		SAFE_SUBTRACT(1,s.probability) as no_plan_shift_probability
	from `edw-prod-153420.eda_enablement__ml.ml_chi_plan_expansion` s 
	join accounts a on cast(a.account_id as string) = s.zendesk_account_id
	where date_scored = (select max(date_scored) from `edw-prod-153420.eda_enablement__ml.ml_chi_plan_expansion`)
)


select
	a.*,
	ae.run_at,
	ae.unique_identifier,
	ae.agent_email,
	ae.agent_id,
	ae.agent_name,
	ae.derived_agent_type,
	ae.agent_language,
	ae.agent_is_active,
	agent_is_verified,
	agent_created_date,
	ai.num_users,
	ai.num_admins,
	ai.num_agents,
	ai.num_light_agents,
	ab.answer_bot_instance_created_date,
	answer_bot_trial_expires_date,
	answer_bot_days_elapsed,
	answer_bot_estimated_resolutions,
	answer_bot_max_resolutions,
	answer_bot_product_state,
	answer_bot_resolutions_used,
	answer_bot_boosted_resolutions,
	answer_bot_boost_expires_at,
	IF(e.account_id is not null,'1','0') AS explore_product_flag,
	e.explore_account_type,
	e.explore_plan_name,
	e.explore_activation_flag,
	e.explore_activation_date,
	No_of_Calls_in_last_30_days__c,
	IVR_Enabled__c,
	cm.support_agents,
	cm.support_email_connector_enabled,
	cm.support_tickets,
	support_one_touch_tickets,
	support_custom_ticket_fields,
	support_tickets_created_last_14_days, 
	support_tickets_created_last_30_days, 
	chat_enabled_date__c,
	talk_cti_partner_plan,
	talk_telephony_Credits_Remaining,
	talk_last_known_plan,
	talk_initial_channel,
	talk_sms_enabled,
	talk_total_agents,
	support_last_login,
	c.chat_conversations_in_last_day,
	plan_shift_model_plan, -- New Field in Q3 
	plan_shift_probability, -- New Field in Q3
	no_plan_shift_probability
from accounts a 
left join agent ae on a.account_id = ae.account_id and ae.run_at = a.latest_run_at -- 41.2S
left join agent_info ai on ai.account_id = a.account_id 
left join answer_bot ab on ab.account_id = a.account_id 
left join explore_accounts e on e.account_id = a.account_id -- 47.2S
left join combo_metrics cm on cm.account_id = a.account_id 
left join chat_metrics c on c.zendesk_account_id = a.account_id 
left join shift s on s.account_id = a.account_id 
order by a.account_id 
