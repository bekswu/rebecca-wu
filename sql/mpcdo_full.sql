with accounts as (
	select 
		a.*,
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
	from (
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
	) a 
	join `edw-prod-153420.pdw.agent_email_addresses` ae on a.account_id = ae.account_id and a.latest_run_at = ae.run_at
	left join `edw-prod-153420.pdw.translation_locales`  t on ae.locale_id = t.locale_id
	where ae.agent_type = 'Admin'
	 	and ae.is_active = 1 
),

agent_info as ( 
	select 
		r.account_id, 
		num_users,
		num_admins, 
		num_agents,
		num_agents/num_admins as agent_to_admin_ratio,
		num_light_agents
	from `edw-prod-153420.pdw.roles` r  
	where r.run_at = (select max(run_at) from `edw-prod-153420.pdw.roles`)
	and r.account_id in (select distinct account_id from accounts)
),

answer_bot as (
	select 
		ab.account_id,
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
		CAST(SAFE_CAST(zacct.zd_id__c as FLOAT64) AS INT64) as account_id,
		zacct.No_of_Calls_in_last_30_days__c,
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
	where CAST(SAFE_CAST(zacct.zd_id__c as FLOAT64) AS INT64) in (select distinct account_id from accounts)
),

chat_accounts as ( -- 52.5S; 14GB
	select 
		i.zendesk_account_id,
		i.voltron_id,
		i.zopim_number,
		coalesce(v2.conversations, c2.conversations) as chat_conversations_in_last_day,
		c3.chat_subdomain,
		c3.chat_instance_created_date,
		c3.chat_derived_account_type,
		c3.chat_win_date,
		c3.chat_churn_date,
		chat_cancellation_date,
		chat_use_case,
		chat_plan_name,
		chat_status,
		chat_phase,
		chat_max_agents,
		chat_enabled_agent_count,
		chat_enabled_triggers,
		chat_web_widget_embedded_date,
		chat_web_widget_embedded_flag,
		v3.forms_pre_chat_form_required as chat_prechat_form_flag, 
		COALESCE(v3.offline_form_facebook,v4.offline_form_facebook) as chat_offline_form_facebook_flag,
		COALESCE(v3.offline_form_twitter,v4.offline_form_twitter) as chat_offline_form_twitter_flag, 
		COALESCE(v3.chat_request_form,v4.chat_request_form) as chat_request_form_flag, 
		COALESCE(v3.routing_skill,v4.routing_skill)  as chat_skill_routing_flag, 
		COALESCE(v3.operating_hours,v4.operating_hours) as chat_operating_hours_flag, 
		COALESCE(v3.chat_button_hide_when_offline, v4.chat_button_hide_when_offline) as chat_offline_form_flag
	from pdw.instances_by_win_and_churn_dates i 
	left join (
		select v.*
		from (
			select 
				v.account_id,
				conversations, 
				RANK()OVER(PARTITION BY v.account_id ORDER BY timestamp DESC) as row
			from  `edw-prod-153420.chat_data.voltron_account_stats` v
			join `pdw.instances_by_win_and_churn_dates` i on i.voltron_id = v.account_id
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
			join `pdw.instances_by_win_and_churn_dates` i on i.zopim_number = c.account_id
		) c
		where c.row = 1 
	) c2 on i.zopim_number = c2.account_id
	left join  (	
		select c.*
		from (
			select 
				c.zendesk_id,
				c.subdomain AS chat_subdomain,
				c.create_date AS chat_instance_created_date,
				c.derived_chat_account_type AS chat_derived_account_type,
				c.chat_win_dt AS chat_win_date,
				c.chat_churn_dt AS chat_churn_date,
				c.cancellation_date AS chat_cancellation_date,
				c.use_case AS chat_use_case,
				c.chat_plan_name AS chat_plan_name,
				c.chat_status AS chat_status,
				c.phase AS chat_phase,
				c.max_agents AS chat_max_agents,
				c.enabled_agent_count AS chat_enabled_agent_count,
				c.enabled_trigger_count AS chat_enabled_triggers, --New Field in Q2
				c.embedded_timestamp AS chat_web_widget_embedded_date,
				CASE WHEN c.embedded_timestamp is null THEN 'not embedded' --updated source table in Q2
				WHEN c.embedded_timestamp is not null THEN 'embedded' -- updated source table in Q2
				END AS chat_web_widget_embedded_flag,
				RANK()OVER(PARTITION BY c.zendesk_id ORDER BY create_date DESC) as row
			from  `edw-prod-153420.pdw.chat_accounts` c
			) c
			where c.row = 1 
	) c3 on c3.zendesk_id = i.zendesk_account_id
	left join (
		select v.*
		from (
			select 
				id, 
				run_at,
				forms_pre_chat_form_required, 
				offline_form_facebook, 
				offline_form_twitter, 
				chat_request_form, 
				routing_skill , 
				operating_hours, 
				chat_button_hide_when_offline, 
				RANK()OVER(PARTITION BY id ORDER BY create_date DESC) as row
			from `zopim.com:dynamic-density-326.chat_daily.voltron_edw_accounts` v
			where v.run_at = (select max(run_at) from `zopim.com:dynamic-density-326.chat_daily.voltron_edw_accounts`) ## why do we need this?
		) v
		where v.row = 1 
	) v3 on i.zendesk_account_id = v3.id
	left join (
		select v3.*
		from (
			select 
				id, 
				run_at,
				forms_pre_chat_form_required, 
				offline_form_facebook, 
				offline_form_twitter, 
				chat_request_form, 
				routing_skill , 
				operating_hours, 
				chat_button_hide_when_offline, 
				RANK()OVER(PARTITION BY id ORDER BY create_date DESC) as row
			from `zopim.com:dynamic-density-326.chat_daily.voltron_edw_accounts`
		) v3
		where v3.row = 1 
	) v4 on i.zopim_number = v4.id
	where (voltron_id is not null or zopim_number is not null or c3.zendesk_id is not null)
),

guide_accounts as (
	select 
		g.account_id,
		g.is_trial AS guide_active_trial_flag,
		g.trial_start_date AS guide_trial_start_date,
		g.trial_end_date AS guide_trial_end_date,
		g.win_dt AS guide_win_date,
		g.churn_dt AS guide_churn_date,
		g.derived_guide_account_type AS guide_derived_account_type,
		g.plan_name AS guide_plan_name,
		g.max_agents AS guide_max_agents,
		g.num_article_edits_28d AS guide_num_article_edits_28d,
		g.num_articles AS guide_num_articles,
		g.num_articles_added_28d AS guide_num_articles_added_28d,
		g.num_hc AS guide_num_hc,
		g.num_hc_archived AS guide_num_hc_archived,
		g.num_hc_embeddables_automatic_answers AS guide_num_hc_embeddables_automatic_answers,
		g.num_hc_embeddables_web_widget AS guide_num_hc_embeddables_web_widget,
		g.num_hc_enabled AS guide_num_hc_enabled,
		g.num_hc_restricted AS guide_num_hc_restricted,
		g.content_cues_activated as guide_content_cues_activated_flag
	from pdw.guide_accounts g 
	where g.account_id is not null	
),

shift as (
	select 
		s.zendesk_account_id,
		s.model_plan as plan_shift_model_plan, 
		s.probability as plan_shift_probability, 
		SAFE_SUBTRACT(1,s.probability) as no_plan_shift_probability
	from `edw-prod-153420.eda_enablement__ml.ml_chi_plan_expansion` s 
	where date_scored = (select max(date_scored) from `edw-prod-153420.eda_enablement__ml.ml_chi_plan_expansion`)
),

churn_prob as (
	select 
		crm_account_id,
		churn_probability, 
		health_score as churn_health_score, 
		model_plan as churn_model_plan 
	from `edw-prod-153420.eda_enablement__ml.ml_chi_current_churn`  
	where date_scored = (select max(date_scored) from `edw-prod-153420.eda_enablement__ml.ml_chi_current_churn` )
),

crm as (
	select 
		a2.id,
		a2.name as company,
		a2.territory_country__c AS sfdc_account_country, 
		a2.region__c AS sfdc_account_region, 
		a2.employee_range__c AS sfdc_account_employee_range,
		min(sales_model) as sales_model ## need market segment rollup? or granular employee ranges 
	from `edw-prod-153420.sfdc.account_scd2` a2 
	--join accounts a on a.sfdc_crm_id = a2.id 
	left join edw_consolidated.customer_dim_scd2 cd on (cd.crm_account_id = a2.id and cd.dw_curr_ind = 'Y')
	where a2.dw_curr_ind = 'Y'
	group by 1,2,3,4,5
)

select distinct 
	a.*,
	crm.company,
	crm.sfdc_account_country,
	crm.sfdc_account_region,
	crm.sfdc_account_employee_range,
	crm.sales_model,
	g.account_id AS guide_account_id,
	v.account_id AS talk_account_id,
	c.zendesk_account_id AS chat_id,
	pql.score as support_pql_score,
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
	-- chat fields
	IF(a.account_id=c.zendesk_account_id,'1','0') AS chat_product_flag, 
	c.chat_conversations_in_last_day,
	c.chat_subdomain,
	c.chat_instance_created_date,
	c.chat_derived_account_type,
	c.chat_win_date,
	c.chat_churn_date,
	c.chat_cancellation_date,
	c.chat_use_case,
	c.chat_plan_name,
	c.chat_status,
	c.chat_max_agents,
	c.chat_enabled_agent_count,
	c.chat_enabled_triggers,
	c.chat_web_widget_embedded_date,
	c.chat_web_widget_embedded_flag,
	c.chat_prechat_form_flag,
	c.chat_offline_form_facebook_flag,
	c.chat_offline_form_twitter_flag,
	c.chat_request_form_flag,
	c.chat_skill_routing_flag,
	c.chat_operating_hours_flag,
	c.chat_offline_form_flag,
	-- guide fields
	IF (a.account_id=g.account_id,'1','0') AS guide_product_flag, 
	g.guide_active_trial_flag,
	g.guide_trial_start_date,
	g.guide_trial_end_date,
	g.guide_win_date,
	g.guide_churn_date,
	g.guide_derived_account_type,
	g.guide_plan_name,
	g.guide_max_agents,
	g.guide_num_article_edits_28d,
	g.guide_num_articles,
	g.guide_num_articles_added_28d,
	g.guide_num_hc,
	g.guide_num_hc_archived,
	guide_num_hc_embeddables_automatic_answers,
	guide_num_hc_embeddables_web_widget,
	guide_num_hc_enabled,
	guide_num_hc_restricted,
	guide_content_cues_activated_flag,
	IF(kc.total_events_28 > 0,1,0) as guide_knowledge_capture_flag,
	-- talk fields
	IF (a.account_id=v.account_id,'1','0') AS talk_product_flag, 
	v.is_trial AS talk_active_trial_flag,
	v.derived_trial_start_date AS talk_trial_start_date,
	v.trial_end_date AS talk_trial_end_date,
	v.num_days_in_trial AS talk_num_days_in_trial,
	v.plan_name AS talk_plan_name,
	v.derived_voice_account_type AS talk_derived_account_type,
	v.win_date AS talk_win_date,
	v.has_voice_subscription AS has_talk_subscription,
	v.is_active AS talk_is_active,
	v.churn_date AS talk_churn_date,
	v.is_active AS talk_active_flag,
	v.plan_name_at_win AS talk_plan_name_at_win,
	plan_shift_model_plan,  
	plan_shift_probability,
	no_plan_shift_probability,
	churn_probability, 
	churn_health_score, 
	churn_model_plan
from accounts a 
left join combo_metrics cm on cm.account_id = a.account_id 
left join chat_accounts c on c.zendesk_account_id = a.account_id 
left join guide_accounts g on g.account_id = a.account_id 
left join pdw.redshift_voice_account v on (v.account_id = a.account_id and v.run_at = a.latest_run_at)
left join agent_info ai on ai.account_id = a.account_id 
left join answer_bot ab on ab.account_id = a.account_id 
left join explore_accounts e on e.account_id = a.account_id -- 47.2S
left join shift s on cast(s.zendesk_account_id as int64) = a.account_id 
left join churn_prob cp on cp.crm_account_id = a.sfdc_crm_id 
left join crm on crm.id = a.sfdc_crm_id
left join `edw-prod-153420.pdw.support_pql` pql on a.account_id = pql.account_id
left join (
	select 
		account_id,
		run_at, 
		MAX(total_events_28) AS total_events_28
	from `edw-prod-153420.pdw.hc_knowledge_capture` kc 
	group by 1,2
) kc on (kc.account_id = a.account_id and a.latest_run_at = kc.run_at)
order by a.account_id 
