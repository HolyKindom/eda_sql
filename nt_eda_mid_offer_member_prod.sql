------------------location-->procedure etl_app.sp_mid_offer_member_prod-----------------------------------------
/*
所需源表
  etl_load.bss_offer_roles   ----生成的配置表
  
  etl_load.bss_offer_member
  etl_load.bss_offer_spec
  etl_load.bss_offer_prod

目标表
  MID_OFFER_MEMBER_PROD  
*/
-- tmp_mid_offer_member_prod
create table tmp_mid_offer_member_prod as
select offer_member_id,
       offer_id,
       offer_role_id,
       obj_type,
       member_id,
       start_dt,
       end_dt,
       status_cd,
       status_dt,
       create_dt
  from etl_load.bss_offer_member aa
 where obj_type = 2
   AND (STATUS_CD <> 22 OR
       STATUS_CD = 22 AND STATUS_DT >=trunc(add_months(sysdate-1,-12)))

-- MID_OFFER_MEMBER_PROD
create table MID_OFFER_MEMBER_PROD as
select a.offer_id,
       a.member_id,
       a.offer_role_id,
       a.start_dt,
       a.end_dt,
       a.status_cd,
       a.status_dt,
       b.OFFER_SPEC_ID,
       b.OFFER_TYPE_CD,
       os.CODE jf_code,
       b.OFFER_SPEC_NAME,
       b.ROLE_CD,
       b.ROLE_CD_NAME,
       b.RULE_DESC,
       b.merge_type,
       b.merge_type1,
       b.merge_type2,
       b.cate_nm_1,
       b.cate_nm_2,
       b.cate_nm_3,
       b.cate_nm_4,
       b.cate_nm_5,
       b.cate_nm_6,
       b.cate_nm_7,
       b.MAIN_OFFER_SEQ,
       c.redu_prod_spec_id,
       c.redu_owner_id,
       c.redu_access_number,
       c.redu_acct_id,
       a.create_dt offer_member_create_dt
  from tmp_mid_offer_member_prod a
 inner join etl_load.bss_offer_roles b on a.offer_role_id = b.offer_role_id
 inner join etl_load.bss_offer_spec os on os.offer_spec_id=b.OFFER_SPEC_ID
 INNER JOIN etl_load.bss_offer_prod c ON a.member_id = c.prod_id