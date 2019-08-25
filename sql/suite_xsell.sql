with accounts as (with non_suite as (
	select s.*
	from `marketing_analyst_general.rw_suite_xsell_v4` s 
  where subregion_dir like '%Velocity%'
  and product_mix_at_cross_sell not in ('Suite')
),

ranks as (
select 
row_number() over (PARTITION BY region_vp order by gb_rank asc) as row,
crm_account_id,
region_vp,
subregion_dir,
gb_rank
from non_suite 
group by 2,3,4,5
),

tiers as (
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'AMER' and row = 4500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'EMEA' and row = 3500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'APAC' and row = 2000 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'LATAM' and row = 1000 group by 1
)

select 
	s.*except(int64_field_0), 
	u.id as sfdc_account_owner_id,
 	u.name as sfdc_account_owner,
 	u2.name as sfdc_account_owner_role,
 	u.email as sfdc_account_owner_email
from non_suite s
left join `edw-prod-153420.sfdc.account_scd2` a on s.crm_account_id = a.id
	and dw_curr_ind = 'Y' 
	and a.isdeleted = 'False'  
left join  `edw-prod-153420.sfdc.user_scd2` u on a.ownerid = u.id 
	and u.dw_curr_ind = 'Y' 
left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	  and u2.dw_curr_ind = 'Y'
where
  region_vp = 'AMER'
  and gb_rank <= (select distinct gb_rank from tiers where region_vp = 'AMER')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39--,40,41
UNION DISTINCT 
(with non_suite as (
	select s.*
	from `marketing_analyst_general.rw_suite_xsell_v4` s 
  where subregion_dir like '%Velocity%'
  and product_mix_at_cross_sell not in ('Suite')
  --in ('Starter-Only', 'Suite')

),

ranks as (
select 
row_number() over (PARTITION BY region_vp order by gb_rank asc) as row,
crm_account_id,
region_vp,
subregion_dir,
gb_rank
from non_suite 
group by 2,3,4,5
),

tiers as (
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'AMER' and row = 4500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'EMEA' and row = 3500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'APAC' and row = 2000 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'LATAM' and row = 1000 group by 1
)

select 
	s.*except(int64_field_0), 
	u.id as sfdc_account_owner_id,
 	u.name as sfdc_account_owner,
 	u2.name as sfdc_account_owner_role,
 	u.email as sfdc_account_owner_email
from non_suite s
left join `edw-prod-153420.sfdc.account_scd2` a on s.crm_account_id = a.id
	and dw_curr_ind = 'Y' 
	and a.isdeleted = 'False'  
left join  `edw-prod-153420.sfdc.user_scd2` u on a.ownerid = u.id 
	and u.dw_curr_ind = 'Y' 
left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	  and u2.dw_curr_ind = 'Y'
where
  region_vp = 'EMEA'
  and gb_rank <= (select distinct gb_rank from tiers where region_vp = 'EMEA')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39--,40,41
)
UNION DISTINCT (
with non_suite as (
	select s.*
	from `marketing_analyst_general.rw_suite_xsell_v4` s 
  where subregion_dir like '%Velocity%'
  and product_mix_at_cross_sell not in ('Suite')
  --in ('Starter-Only', 'Suite')

),

ranks as (
select 
row_number() over (PARTITION BY region_vp order by gb_rank asc) as row,
crm_account_id,
region_vp,
subregion_dir,
gb_rank
from non_suite 
group by 2,3,4,5
),

tiers as (
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'AMER' and row = 4500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'EMEA' and row = 3500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'APAC' and row = 2000 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'LATAM' and row = 1000 group by 1
)

select 
	s.*except(int64_field_0), 
	u.id as sfdc_account_owner_id,
 	u.name as sfdc_account_owner,
 	u2.name as sfdc_account_owner_role,
 	u.email as sfdc_account_owner_email
from non_suite s
left join `edw-prod-153420.sfdc.account_scd2` a on s.crm_account_id = a.id
	and dw_curr_ind = 'Y' 
	and a.isdeleted = 'False'  
left join  `edw-prod-153420.sfdc.user_scd2` u on a.ownerid = u.id 
	and u.dw_curr_ind = 'Y' 
left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	  and u2.dw_curr_ind = 'Y'
where
  region_vp = 'APAC'
  and gb_rank <= (select distinct gb_rank from tiers where region_vp = 'APAC')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39--,40,41
)
union distinct (
with non_suite as (
	select s.*
	from `marketing_analyst_general.rw_suite_xsell_v4` s 
  where subregion_dir like '%Velocity%'
  and product_mix_at_cross_sell not in ('Suite')
  --in ('Starter-Only', 'Suite')

),

ranks as (
select 
row_number() over (PARTITION BY region_vp order by gb_rank asc) as row,
crm_account_id,
region_vp,
subregion_dir,
gb_rank
from non_suite 
group by 2,3,4,5
),

tiers as (
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'AMER' and row = 4500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'EMEA' and row = 3500 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'APAC' and row = 2000 group by 1
union distinct 
select region_vp, min(gb_rank) as gb_rank from ranks where region_vp = 'LATAM' and row = 1000 group by 1
)

select 
	s.*except(int64_field_0), 
	u.id as sfdc_account_owner_id,
 	u.name as sfdc_account_owner,
 	u2.name as sfdc_account_owner_role,
 	u.email as sfdc_account_owner_email
from non_suite s
left join `edw-prod-153420.sfdc.account_scd2` a on s.crm_account_id = a.id
	and dw_curr_ind = 'Y' 
	and a.isdeleted = 'False'  
left join  `edw-prod-153420.sfdc.user_scd2` u on a.ownerid = u.id 
	and u.dw_curr_ind = 'Y' 
left join `sfdc.userrole_scd2` u2 on u2.id = u.userroleid
  	  and u2.dw_curr_ind = 'Y'
where
  region_vp = 'LATAM'
  and gb_rank <= (select distinct gb_rank from tiers where region_vp = 'LATAM')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39--,40,41
))

select 
a.crm_account_id,
a.region_vp,
a.subregion_dir,
a.sfdc_account_owner,
a.sfdc_account_owner_role,
a.sfdc_account_owner_email,
ae.agent_name,
ae.agent_type,
ae.agent_email,
c.id as sfdc_contact_id
from accounts a 
left join  `edw-prod-153420.sfdc.contact_scd2` c on a.crm_account_id = c.accountid
  and c.dw_curr_ind = "Y" 
 and c.isdeleted = "False"
left join `pdw.agent_email_addresses` ae on c.email = ae.agent_email 
where ae.is_active = 1
and ae.agent_type = 'Admin'
group by 1,2,3,4,5,6,7,8,9,10


