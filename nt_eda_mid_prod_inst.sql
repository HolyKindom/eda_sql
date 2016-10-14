/*
  目标表：mid_prod_inst
*/
------------------location-->package body etl_app.PKG_MID_PROD_INST-----------------------------------------
---------------------------------------procedure:SP_MID_BILL_ACCT_ITEM--------------------------------------
--1 TMP_MID_OWE_AGE_DETAIL
  --1.1 TMP_MID_OWE_AGE_DETAIL
CREATE TABLE TMP_MID_OWE_AGE_DETAIL AS
   SELECT  /*+ parallel(A,4) parallel(B,4) use_hash(A,B)*/ 
           A.SERV_ID,
           A.BILLING_CYCLE_ID,
           AMOUNT,
           BILLING_MODE_ID
   FROM 
           ETL_LOAD.BIlL_ACCT_ITEM_TEMP A
   LEFT JOIN  
           etl_load.BILL_SERV B 
           ON 
           A.SERV_ID=B.SERV_ID

  --1.2
DELETE /*+ PARALLEL(A,4)*/
       FROM TMP_MID_OWE_AGE_DETAIL
       WHERE BILLING_MODE_ID = 1
       AND BILLING_CYCLE_ID =tO_number(TO_CHAR(ADD_MONTHS(TO_DATE(TO_CHAR(SYSDATE, ' YYYYMM '),' YYYYMM '),-1),' YYYYMM '))

  --1.3
alter table TMP_MID_OWE_AGE_DETAIL rename to BILL_ACCT_ITEM


--2 tmp_mid_prod_inst_owe
create table tmp_mid_prod_inst_owe as
      select a.serv_id,
         count(distinct a.billing_cycle_id) owe_months,
         min(a.billing_cycle_id) min_owe_month,
         sum(a.amount) owe_charge
      from 
          BILL_ACCT_ITEM a
      group by a.serv_id
  
---------------------------------------procedure:SP_MID_PROD_INST_BASE--------------------------------------
--3 tmp_mid_prod_inst_base
  --3.1 
create table tmp_mid_prod_inst_base as
select a.pd_inst_id prod_id,
       a.main_accs_nmbr access_number,
       a.blng_accs_nmbr acc_nbr,
       a.pd_spec_id prod_spec_id,
       a.instl_addr_desc prod_addr,
       b.PD_SPEC_NM PROD_SPEC_NM,
       L.CATE_NM_2  PROD_TYPE,
       a.area_id area_id,
       c.admin_lo_nm area_name,
       a.acpt_staff_id sl_staff_id,
       d.empl_cd sl_staff_number,
       d.empl_nm sl_staff_name,
       a.assist_id xx_staff_id,
       a.assist_cd xx_staff_number,
       e.empl_nm xx_staff_name,
       a.crtd_dt  ,
       a.start_dt start_dt,
       a.end_dt end_dt,
       a.busi_st busi_st,
       f.d_nm bss_st_name,
       a.chnl_id bss_channel_id,
       g.nm bss_channel_name,
       a.agent_chnl_id agent_chnl_id,
       a.dvlp_agent_chnl_id,
       h.agent_chnl_nm agent_chnl_nm,
       a.cntry_f css_city_id,
       a.mana_id manager_id,
       i.nm cust_manager_name,
       a.rsrc_accs_tp_id acc_tp,
       a.acct_id acct_id,
       a.cust_id owner_id,
       a1.nm owner_name,
       a.cust_tp_id segment_id,
       case
         when a.cust_tp_id in (6, 7, 9) then
          '家庭客户群'
         else
          '政企客户群'
       end fq_name,
       a.mkt_tgt_id css_cust_id,
       a.mkt_tgt_tp_id custom_type_id,
       j.mkt_tgt_tp_nm custom_type_name,
       a.card_f hk_flag,
       case
         when a.card_active_dt is not null then
          1
       end hk_IS_ACTIVE,
       a.card_active_dt hk_state_date,
       a.exist_tp_id exist_tp_id,
       a.blng_mode billing_mode_id,
       a.blng_user_f blng_user_f,
       a.online_f online_f,
       case when a.end_dt>sysdate then
       round(months_between(trunc(sysdate), nvl(a.start_dt,trunc(sysdate))), 0) else
       round(months_between(nvl(a.end_dt,trunc(sysdate)）,nvl(a.start_dt,trunc(sysdate))), 0)  end    on_innet_length,
       a.last_innet_dt last_innet_dt,
       a.last_lv_dt last_lv_dt,
       case
         when decode(a1.main_cust_idnty_tp_id,-1,9999,a1.main_cust_idnty_tp_id) < decode(a1.other_cust_idnty_tp_id,-1,9999,a1.other_cust_idnty_tp_id) then
          decode(a1.main_cust_idnty_tp_id,
              1,'身份证',2,'军官证',3,'税务证',
              4,'工商注册号',5,'电信识别号',
              6,'驾驶证',7,'教师证',8,'学生证',
              9,'护照',10,'社保卡',11,'组织机构代码',
              12,'客户卡',13,'客户标识码',17,'老年证',
              18,'集团客户标识码(jtcrmid)','其他')
         else
          decode(a1.other_cust_idnty_tp_id,
              1,'身份证',2,'军官证',3,'税务证',
              4,'工商注册号',5,'电信识别号',
              6,'驾驶证',7,'教师证',8,'学生证',
              9,'护照',10,'社保卡',11,'组织机构代码',
              12,'客户卡',13,'客户标识码',17,'老年证',
              18,'集团客户标识码(jtcrmid)','其他')
       end IDENTITY_TYPE,
       case when decode(a1.main_cust_idnty_tp_id,-1,9999,a1.main_cust_idnty_tp_id) < decode(a1.other_cust_idnty_tp_id,-1,9999,a1.other_cust_idnty_tp_id) then
         a1.main_cust_idnty
         else
         a1.other_cust_idnty
       end  IDENTITY_NUM,
       a.chrg_f chrg_f,
       a.basic_po_spec_id BASIC_OFFER_CODE,
       k.po_spec_nm BASIC_OFFER_NAME
  from bml.pd_d a
  left join bml.CUST_DAILY_INFO a1 on a.cust_id = a1.cust_id
  left join BML.PD_SPEC_D b on a.pd_spec_id = b.pd_spec_id
  LEFT JOIN BML.PD_SPEC_CATE_TREE L on b.dflt_pd_spec_cate_id = L.pd_spec_cate_id 
  left join bml.admin_lo_d c on a.area_id = c.admin_lo_id
  left join bml.empl d on a.acpt_staff_id = d.empl_id
  left join bml.empl e on a.assist_id = e.empl_id
  left join bml.common_d f on a.busi_st = f.d_cd
                          and d_schm_id = 212001
  left join bml.chnl_d g on a.chnl_id = g.chnl_id
  left join bml.agent_chnl_d h on a.agent_chnl_id = h.agent_chnl_id
  left join bml.cust_mana i on a.mana_id = i.mana_id
  left join bml.mkt_tgt_tp_d j on a.mkt_tgt_tp_id = j.mkt_tgt_tp_id
  left join bml.po_spec_d k on a.basic_po_spec_id = k.po_spec_id


  --4  tmp_mid_prod_inst_css
    --4.1
  create table tmp_mid_prod_inst_css as
select t.pd_inst_id,
       a.speed_id,
       t1.speed_value / 1024 KD_RAX,
       f.mkt_lo_id dvlp_area_id,
       t.dvlp_grid_id dvlp_grid_id,
       t.phy_grid_id phy_grid_id,
       l.mkt_lo_id PHY_AREA_ID,
       t.MKT_TGT_TP_ID grid_claim_type,
       t.grid_id css_grid_id, ---考核网格id
       b.grid_nm css_grid_name,
       t.mkt_lo_id manager_area_id,
       t.mana_id css_manager_id,
       c.nm css_manager_name,
       h.mkt_lo_nm_4 sub_area_name,
       h.mkt_lo_nm_5 sub_branch_name,
       e.mkt_lo_nm_4 FEE_AREA_NAME,
       e.mkt_lo_id_5 BRANCH_ID,
       e.mkt_lo_nm_5 BRANCH_NAME
  from bml.pd_d t
  left join bml.bss_pd_d a on t.pd_inst_id = a.pd_inst_id
  left join bml.speed_d t1 on a.speed_id = t1.speed_id
  left join bml.grid_d l on t.phy_grid_id = l.grid_id
  left join bml.grid_d b on t.grid_id = b.grid_id
  left join bml.cust_mana c on t.mana_id = c.mana_id
  left join bml.mkt_lo_d e1 on t.mkt_lo_id = e1.mkt_lo_id
  left join bml.lv4_mkt_lo_d e on e1.lv5_mkt_lo_id = e.lv5_mkt_lo_id
  left join bml.grid_d f on f.grid_id = t.dvlp_grid_id
  left join bml.mkt_lo_d g on f.mkt_lo_id = g.mkt_lo_id
  left join bml.lv4_mkt_lo_d h on g.lv5_mkt_lo_id = h.lv5_mkt_lo_id
  where t.start_dt< trunc(sysdate-1) or t.start_dt is null
      
    --4.2
    insert into tmp_mid_prod_inst_css
select t.pd_inst_id,
       a.speed_id,
       t1.speed_value / 1024 KD_RAX,
          case
         when t3.staff_id is not null then
          t3.area_id
         when t3.staff_id is null and t5.channel_id is not null then
          t5.Area_Id
         else
          null
       end dvlp_area_id,
       t.dvlp_grid_id dvlp_grid_id,
       t.phy_grid_id phy_grid_id,
       l.mkt_lo_id PHY_AREA_ID,
       t.MKT_TGT_TP_ID grid_claim_type,
       t.grid_id css_grid_id, ---考核网格id
       b.grid_nm css_grid_name,
       t.mkt_lo_id manager_area_id,
       t.mana_id css_manager_id,
       c.nm css_manager_name,
       case
         when t3.staff_id is not null then
          t3.big_area_name
         when t3.staff_id is null and t5.channel_id is not null then
          t5.big_area_name
         else
          null
       end sub_area_name,
       case
         when t3.staff_id is not null then
          t3.branch_name
         when t3.staff_id is null and t5.channel_id is not null then
          t5.branch_name
         else
          null
       end sub_branch_name,
       e.mkt_lo_nm_4 FEE_AREA_NAME,
       e.mkt_lo_id_5 BRANCH_ID,
       e.mkt_lo_nm_5 BRANCH_NAME
  from bml.pd_d t
  left join bml.bss_pd_d a on t.pd_inst_id = a.pd_inst_id
  left join bml.speed_d t1 on a.speed_id = t1.speed_id
  left join bml.grid_d l on t.phy_grid_id = l.grid_id
  left join bml.grid_d b on t.grid_id = b.grid_id
  left join bml.cust_mana c on t.mana_id = c.mana_id
  left join bml.mkt_lo_d e1 on t.mkt_lo_id = e1.mkt_lo_id
  left join bml.lv4_mkt_lo_d e on e1.lv5_mkt_lo_id = e.lv5_mkt_lo_id
  left join cfg_zx_staff_area t3 on t.assist_id = t3.staff_id
  left join bml.agent_chnl_d t4 on t.agent_chnl_id = t4.agent_chnl_id
  left join cfg_zx_channel_area t5 on t4.bss_chnl_id = t5.channel_id
  where t.start_dt >= trunc(sysdate -1)


    --5 tmp_mid_prod_inst_jf  计费信息(状态)
    create table tmp_mid_prod_inst_jf as
      select serv_id,
             state JF_STATE,
             decode(state,
              '2HA',
              '正常',
              '2HB',
              '注销',
              '2HC',
              '用户要求停机',
              '2HD',
              '双停',
              '2HE',
              '强制停机',
              '2HF',
              '已归档',
              '2HN',
              '新装未激活',
              '2HS',
              '单停',
              '2HX',
              '欠费被拆机',
              '2ID',
              '停保双停',
              '2IS',
              '停保单停',
              '2IX',
              '用户申请拆机',
              '2SX',
              '预拆机',
              '其它')  JF_STATE_NAME,
       state_date jf_state_date,
       create_date jf_create_date
from etl_load.bill_serv a

    --6 --vpn号码信息
    CREATE TABLE MD_VPN_OFFER_VALUE AS
            select 
               t1.prod_id
            from ETL_LOAD.BSS_PROD_SPEC t,
                ETl_load.Bss_Product t1
        where t.prod_spec_id = t1.prod_spec_id
          and (t.name like '%虚拟%' or t.PROD_SPEC_ID in  (377 ,331))


    --7 MD_VPN_OFFER_VALUE_1
    CREATE TABLE MD_VPN_OFFER_VALUE_1 AS
     SELECT 
         T1.SUB_PROD_ID,
         MAX(T1.COMP_PROD_ID) COMP_PROD_ID 
     FROM 
         MD_VPN_OFFER_VALUE T
     INNER JOIN  
           ETL_LOAD.BSS_COMP_PROD T1  
     ON  T.PROD_ID = T1.COMP_PROD_ID
     GROUP BY T1.SUB_PROD_ID

     --8 tmp_mid_prod_inst_vpn
     create table tmp_mid_prod_inst_vpn as
        select t.sub_prod_id         serv_id,
               t2.redu_access_number net_nbr,
               t3.name ivpn_name
          from MD_VPN_OFFER_VALUE_1 t
          left join etl_load.bss_offer_prod t2
            on t.comp_prod_id = t2.prod_id
          left join etl_load.bss_party t3
            on t2.redu_owner_id = t3.party_id

 
     --9  tmp1_mid_prod_inst_hb
     create table tmp1_mid_prod_inst_hb as
        select a.*,
               b.SPEED_ID,
               b.KD_RAX,
               b.DVLP_AREA_ID,
               b.DVLP_GRID_ID,
               b.PHY_GRID_ID,
               b.PHY_AREA_ID,
               b.GRID_CLAIM_TYPE,
               b.CSS_GRID_ID,
               b.CSS_GRID_NAME,
               b.MANAGER_AREA_ID,
               b.CSS_MANAGER_ID,
               b.CSS_MANAGER_NAME,
               b.SUB_AREA_NAME,
               b.SUB_BRANCH_NAME,
               b.FEE_AREA_NAME,
               b.BRANCH_ID,
               b.BRANCH_NAME,
               c.jf_state,
               c.jf_state_name,
               c.JF_STATE_DATE,
               c.JF_CREATE_DATE
          from tmp_mid_prod_inst_base a
          left join tmp_mid_prod_inst_css b on a.prod_id = b.pd_inst_id
          left join tmp_mid_prod_inst_jf c on a.prod_id = c.serv_id


     --10 --取BSS宽带速率VALUE
     CREATE TABLE PROD_INST_KDSL_TMP AS
             WITH T2 AS 
             (SELECT 
                  prod_id,
                  STATUS_CD,
                  CREATE_DT,
                  NAME,
                  rn,
                  VALUE,
                  ROW_NUMBER() 
                  OVER(PARTITION BY prod_id,STATUS_CD ORDER BY END_DT DESC NULLS LAST) rn1
                 FROM  (
                       SELECT 
                         a.prod_id,
                         A.STATUS_CD,
                         A.CREATE_DT,
                         B.NAME,
                         B.VALUE,
                         A.END_DT,
                        RANK() OVER(PARTITION BY A.PROD_ID ORDER BY DECODE(A.STATUS_CD,'11',1,'10',2,'12',3,'21',4,'20',5,'22',6) ) rn
                        FROM etl_load.BSS_OFFER_PROD_ITEM a
                        inner join etl_load.BSS_DISCRETE_VALUE_LIST b on a.item_spec_id=b.item_spec_id and a.value=b.meta_key_value
                        where a.ITEM_SPEC_ID = 12
                        and a.status_cd in (10,11,12,21,22,20)) DD )
               SELECT prod_id,STATUS_CD,NAME,VALUE/1024 VALUE FROM T2 WHERE rn=1 AND RN1=1

    --11
    create table tmp2_mid_prod_inst_hb as
    select a.*,
           b.owe_charge,
           b.min_owe_month,
           b.OWE_MONTHS,
           c.net_nbr ,
           c.ivpn_name,
           d.card_3g_f,
           d.GROUP_4G_F,
           KD.VALUE BSS_KD_RAX 
      from tmp1_mid_prod_inst_hb a
      left join tmp_mid_prod_inst_owe b on a.prod_id = b.serv_id
      left join tmp_mid_prod_inst_vpn c on a.prod_id = c.serv_id
      left join bml.pd_mbl_d d on a.prod_id = d.pd_inst_id 
      left join PROD_INST_KDSL_TMP KD on a.prod_id = KD.prod_id


---------------------------------------procedure:SP_MID_PROD_INST_OFFER--------------------------------------
  --12 tmp_mid_prod_inst_mp 打产品甲乙种，主要是固话和宽带
  create table tmp_mid_prod_inst_mp as
  select *
      from (select a.member_id prod_id,
                   a.merge_type1 prod_spec,
                   row_number() over(partition by a.member_id order by a.start_dt) req
              from mid_offer_member_prod A
             where a.merge_type1 in ('甲种固话', '乙种固话', '甲种宽带', '乙种宽带'))
     where req = 1

  --13 tmp_mid_prod_inst_main
  create table tmp_mid_prod_inst_main as
  select *
    from (select /* +PARALLEL(b,4)*/
           b.offer_id PRI_OFFER_ID,
           b.offer_spec_id PRI_OFFER_SPEC_ID,
           B.OFFER_SPEC_NAME PRI_OFFER_SPEC_NAME,
           b.offer_role_id PRI_OFFER_ROLE_ID,
           b.START_DT PRI_START_DT ,
           b.end_dt PRI_END_DT,
           b.role_cd PRI_ROLE_CD,
           b.member_id,
           merge_type,
           status_cd PRI_STATUS_CD,
           row_number() over(partition by b.member_id order by a.cmda_prior_level, b.status_cd, trunc(b.status_dt, 'mm') desc, b.status_dt desc) req
            from mid_offer_member_prod b, v_cfg_cdma_kd_main_spec a
           where b.redu_prod_spec_id  in ( 379,804,805) and b.status_cd in (12,21)
                and  a.offer_spec_id = b.offer_spec_id
                and (a.cmda_prior_level in (101, 501)
              or (a.cmda_prior_level = 201 and b.role_cd in (1, 240, 256, 266, 309))))
   where req = 1

      --13.1 tmp_mid_prod_inst_main
      insert into tmp_mid_prod_inst_main
    select *
      from (select /* +PARALLEL(b,4)*/
             b.offer_id,
             b.offer_spec_id,
             B.OFFER_SPEC_NAME,
             b.offer_role_id,
             b.START_DT,
             b.end_dt,
             b.role_cd,
             b.member_id,
             merge_type,
             status_cd,
             row_number() over(partition by b.member_id order by a.kd_prior_level,  b.status_cd, trunc(b.status_dt, 'mm') desc, b.status_dt desc) req
              from mid_offer_member_prod b, v_cfg_cdma_kd_main_spec a
             where b.redu_prod_spec_id in(9,10,11,12,13,14) and b.status_cd in (12,21) and     a.offer_spec_id = b.offer_spec_id
               and  a.kd_prior_level is not null)
     where req = 1

      --13.2 tmp_mid_prod_inst_main
      insert into tmp_mid_prod_inst_main
            select *
              from (select /* +PARALLEL(b,4)*/
                     b.offer_id,
                     b.offer_spec_id,
                     B.OFFER_SPEC_NAME,
                     b.offer_role_id,
                     b.START_DT,
                     b.end_dt,
                     b.role_cd,
                     b.member_id,
                     merge_type,
                     status_cd,
                     row_number() over(partition by b.member_id order by  b.status_cd, trunc(b.status_dt, 'mm') desc, b.status_dt desc) req
                      from mid_offer_member_prod b
                     where b.redu_prod_spec_id = 881 and b.offer_spec_id in (
                     300509027681,300509029463,300509028699,300509030173,300509030176)
                     and b.status_cd in (12,21) )
                      where req = 1


  --14 tmp_mid_prod_inst_main_old
  create table tmp_mid_prod_inst_main_old as
  select *
            from (select /* +PARALLEL(b,4)*/
                   b.offer_id PRI_OFFER_ID,
                   b.offer_spec_id PRI_OFFER_SPEC_ID,
                   B.OFFER_SPEC_NAME PRI_OFFER_SPEC_NAME,
                   b.offer_role_id PRI_OFFER_ROLE_ID,
                   b.START_DT PRI_START_DT ,
                   b.end_dt PRI_END_DT,
                   b.role_cd PRI_ROLE_CD,
                   b.member_id,
                   merge_type,
                   status_cd PRI_STATUS_CD,
                   row_number() over(partition by b.member_id order by  trunc(b.end_dt, 'mm') desc, a.cmda_prior_level desc) req
                    from mid_offer_member_prod b, v_cfg_cdma_kd_main_spec a
                   where b.redu_prod_spec_id in ( 379,804,805)  and b.status_cd in (22)  and b.start_dt<b.end_dt
                        and  a.offer_spec_id = b.offer_spec_id
                        and (a.cmda_prior_level in (101, 501)
                      or (a.cmda_prior_level = 201 and b.role_cd in (1, 240, 256, 266, 309))))
               
           where req = 1

      --14.1 tmp_mid_prod_inst_main_old
      insert into tmp_mid_prod_inst_main_old
      select *
        from (select /* +PARALLEL(b,4)*/
               b.offer_id,
               b.offer_spec_id,
               B.OFFER_SPEC_NAME,
               b.offer_role_id,
               b.START_DT,
               b.end_dt,
               b.role_cd,
               b.member_id,
               merge_type,
               status_cd,
               row_number() over(partition by b.member_id order by  trunc(b.end_dt, 'mm') desc ,a.kd_prior_level,b.STATUS_DT desc) req
                from mid_offer_member_prod b, v_cfg_cdma_kd_main_spec a
               where b.redu_prod_spec_id in(9,10,11,12,13,14)   
                 and b.start_dt<b.end_dt and b.status_cd in (22) and     a.offer_spec_id = b.offer_spec_id
                 and  a.kd_prior_level is not null)
       where req = 1

      --14.2 tmp_mid_prod_inst_main_old
      insert into tmp_mid_prod_inst_main_old
      select *
        from (select /* +PARALLEL(b,4)*/
                 b.offer_id,
                 b.offer_spec_id,
                 B.OFFER_SPEC_NAME,
                 b.offer_role_id,
                 b.START_DT,
                 b.end_dt,
                 b.role_cd,
                 b.member_id,
                 merge_type,
                 status_cd,
                 row_number() over(partition by b.member_id order by  trunc(b.end_dt, 'mm') desc,  b.STATUS_DT desc) req
                  from mid_offer_member_prod b
                 where b.redu_prod_spec_id = 881 and     b.start_dt<b.end_dt and b.offer_spec_id in (
                 300509027681,300509029463,300509028699,300509030173,300509030176)
                 and b.status_cd in (22) )
                  where req = 1


  --15  tmp_mid_prod_inst_main_new
  create table tmp_mid_prod_inst_main_new as
  select *
      from (select /* +PARALLEL(b,4)*/
             b.offer_id PRI_OFFER_ID,
             b.offer_spec_id PRI_OFFER_SPEC_ID,
             B.OFFER_SPEC_NAME PRI_OFFER_SPEC_NAME,
             b.offer_role_id PRI_OFFER_ROLE_ID,
             b.START_DT PRI_START_DT ,
             b.end_dt PRI_END_DT,
             b.role_cd PRI_ROLE_CD,
             b.member_id,
             merge_type,
             status_cd PRI_STATUS_CD,
             row_number() over(partition by b.member_id order by a.cmda_prior_level, b.status_cd,
              trunc(b.status_dt, 'mm') desc, b.status_dt desc) req
              from mid_offer_member_prod b, v_cfg_cdma_kd_main_spec a
             where b.redu_prod_spec_id in ( 379,804,805)  and b.status_cd in (10,11)
                  and  a.offer_spec_id = b.offer_spec_id
                  and (a.cmda_prior_level in (101, 501)
                or (a.cmda_prior_level = 201 and b.role_cd in (1, 240, 256, 266, 309))))
     where req = 1

      --15.1 tmp_mid_prod_inst_main_new
      insert into tmp_mid_prod_inst_main_new
      select *
        from (select /* +PARALLEL(b,4)*/
               b.offer_id,
               b.offer_spec_id,
               B.OFFER_SPEC_NAME,
               b.offer_role_id,
               b.START_DT,
               b.end_dt,
               b.role_cd,
               b.member_id,
               merge_type,
               status_cd,
               row_number() over(partition by b.member_id order by a.kd_prior_level, b.status_cd,
                trunc(b.status_dt, 'mm') desc, b.status_dt desc ) req
                from mid_offer_member_prod b, v_cfg_cdma_kd_main_spec a
               where b.redu_prod_spec_id in(9,10,11,12,13,14) and b.status_cd in (10,11) and     a.offer_spec_id = b.offer_spec_id
                 and  a.kd_prior_level is not null)
       where req = 1

      --15.2 tmp_mid_prod_inst_main_new
      insert into tmp_mid_prod_inst_main_new
      select *
        from (select /* +PARALLEL(b,4)*/
               b.offer_id,
               b.offer_spec_id,
               B.OFFER_SPEC_NAME,
               b.offer_role_id,
               b.START_DT,
               b.end_dt,
               b.role_cd,
               b.member_id,
               merge_type,
               status_cd,
               row_number() over(partition by b.member_id order by    b.status_cd,
                trunc(b.status_dt, 'mm') desc, b.status_dt desc ) req
                from mid_offer_member_prod b
               where b.redu_prod_spec_id = 881 and b.offer_spec_id in (
               300509027681,300509029463,300509028699,300509030173,300509030176)
               and b.status_cd in (10,11) )
                where req = 1

  --16 tmp_mid_prod_inst_ehome   '我的E家'数据先存到mid_prod_inst_ehome_tmp2中
  create table tmp_mid_prod_inst_ehome as
  select *
    from (select b.MEMBER_ID,
                 B.offer_id EHOME_PO_INST_ID,
                 B.offer_spec_id EHOME_PO_SPEC_ID,
                 b.OFFER_SPEC_NAME ehome_po_spec_name,
                 b.STATUS_CD EHOME_PO_STATUS_CD,
                 B.status_dt EHOME_PO_SPEC_DT,
                 B.offer_role_id EHOME_MEMBER_ROLE_ID,
                 ROW_NUMBER() OVER(PARTITION BY b.MEMBER_ID ORDER BY DECODE(b.status_cd, '12', 1, '20', 2, '21', 3, '13', 4, '10', 5, '11', 6, '22', 7)) req
            from mid_offer_member_prod b
           where cate_nm_2 = '我的E家'
                and  b.start_dt<b.end_dt)
   where req = 1

  --17 tmp_mid_prod_inst_NVGTN  商务领航 数据先存到mid_prod_inst_NVGTN_tmp2中
  create table tmp_mid_prod_inst_NVGTN as
  select *
    from (select /*+PARALLEL(b,4)*/
           b.MEMBER_ID,
           B.offer_id NVGTN_PO_INST_ID,
           B.offer_spec_id NVGTN_PO_SPEC_ID,
           b.OFFER_SPEC_NAME NVGTN_PO_SPEC_NAME,
           b.STATUS_CD NVGTN_PO_STATUS_CD,
           B.status_dt NVGTN_PO_SPEC_DT,
           B.offer_role_id NVGTN_MEMBER_ROLE_ID,
           ROW_NUMBER() OVER(PARTITION BY b.MEMBER_ID ORDER BY DECODE(b.status_cd, '12', 1, '20', 2, '21', 3, '13', 4, '10', 5, '11', 6, '22', 7)) req
            from mid_offer_member_prod b
           where cate_nm_2 = '商务领航'
             and  b.start_dt<b.end_dt)
   where req = 1

  --18 tmp_mid_prod_inst_STDNT  学子E行数据取出 要将天翼和宽带都取出来
  create table tmp_mid_prod_inst_STDNT as
  with jjj_temp as (select  offer_id
                      from (select /*+PARALLEL(b,4)*/
                             b.offer_id,
                             ROW_NUMBER() OVER(PARTITION BY b.MEMBER_ID ORDER BY DECODE(b.status_cd,'12',1,'20',2,'21',3,'13',4,'10',5,'11',6,'22',7)) req
                                        from mid_offer_member_prod b
                                       where cate_nm_2 = '学子E行'
                                         and redu_prod_spec_id = 789
                                         and b.start_dt<b.end_dt)
                               where req = 1
                               group by offer_id)
  select t.MEMBER_ID,
           t.E_STDNT_PO_INST_ID,
           t.E_STDNT_PO_SPEC_ID,
           t.E_STDNT_po_SPEC_NAME,
           t.E_STDNT_PO_STATUS_CD,
           t.E_STDNT_PO_SPEC_DT,
           t.E_STDNT_MEMBER_ROLE_ID from
          (select /*+PARALLEL(b,4)*/
           b.MEMBER_ID,
           B.offer_id        E_STDNT_PO_INST_ID,
           B.offer_spec_id   E_STDNT_PO_SPEC_ID,
           b.OFFER_SPEC_NAME E_STDNT_po_SPEC_NAME,
           b.STATUS_CD       E_STDNT_PO_STATUS_CD,
           B.status_dt       E_STDNT_PO_SPEC_DT,
           B.offer_role_id   E_STDNT_MEMBER_ROLE_ID,
           ROW_NUMBER() OVER(PARTITION BY b.MEMBER_ID ORDER BY DECODE(b.status_cd,'12',1,'20',2,'21',3,'13',4,'10',5,'11',6,'22',7)) req
            from mid_offer_member_prod b
             join jjj_temp c on b.offer_id=c.offer_id
           where b.cate_nm_2 = '学子E行'
             and b.start_dt<b.end_dt) t where t.req=1


  --19 tmp1_mid_prod_inst_vpn  综合虚拟网数据
  create table tmp1_mid_prod_inst_vpn as
  select *
    from (select /*+PARALLEL(b,4)*/
            b.MEMBER_ID,
             B.offer_id vpn_PO_INST_ID,
             B.offer_spec_id vpn_PO_SPEC_ID,
             b.OFFER_SPEC_NAME vpn_po_SPEC_NAME,
             b.STATUS_CD vpn_PO_STATUS_CD,
             B.status_dt vpn_PO_SPEC_DT,
            ROW_NUMBER() OVER(PARTITION BY b.MEMBER_ID ORDER BY DECODE(b.status_cd,'12',1,'20',2,'21',3,'13',4,'10',5,'11',6,'22',7),
            decode(cate_nm_3,'综合虚拟网',1,'总机服务',2,'新乡亲网',3)) req
       from mid_offer_member_prod b 
      where cate_nm_3 in ('综合虚拟网','总机服务','新乡亲网')
        and b.start_dt<b.end_dt) where req=1


  --20 tmp3_mid_prod_inst_hb  合并
  create table tmp3_mid_prod_inst_hb as
  select a.pd_inst_id,
         b.prod_spec,

         h.pri_offer_id o_pri_offer_id,
         h.pri_offer_spec_id o_pri_offer_spec_id,
         h.pri_offer_spec_name o_pri_offer_spec_name,
         h.pri_offer_role_id o_pri_offer_role_id,
         h.pri_start_dt o_pri_start_dt,
         h.pri_end_dt o_pri_end_dt,
         h.pri_role_cd o_pri_role_cd,
         h.pri_status_cd o_pri_status_cd,

         c.pri_offer_id,
         c.pri_offer_spec_id,
         c.pri_offer_spec_name,
         c.pri_offer_role_id,
         c.pri_start_dt,
         c.pri_end_dt,
         c.pri_role_cd,
         c.pri_status_cd,

         i.pri_offer_id n_pri_offer_id,
         i.pri_offer_spec_id n_pri_offer_spec_id,
         i.pri_offer_spec_name n_pri_offer_spec_name,
         i.pri_offer_role_id n_pri_offer_role_id,
         i.pri_start_dt n_pri_start_dt,
         i.pri_end_dt n_pri_end_dt,
         i.pri_role_cd n_pri_role_cd,
         i.pri_status_cd n_pri_status_cd,


         d.ehome_po_inst_id,
         d.ehome_po_spec_id,
         d.ehome_po_spec_name,
         d.ehome_po_status_cd,
         d.ehome_po_spec_dt,
         d.ehome_member_role_id,
         e.nvgtn_po_inst_id,
         e.nvgtn_po_spec_id,
         e.nvgtn_po_spec_name,
         e.nvgtn_po_status_cd,
         e.nvgtn_po_spec_dt,
         e.nvgtn_member_role_id,
         f.e_stdnt_po_inst_id,
         f.e_stdnt_po_spec_id,
         f.e_stdnt_po_spec_name,
         f.e_stdnt_po_status_cd,
         f.e_stdnt_po_spec_dt,
         f.e_stdnt_member_role_id,
         g.vpn_po_inst_id,
         g.vpn_po_spec_id,
         g.vpn_po_spec_name,
         g.vpn_po_status_cd,
         g.vpn_po_spec_dt
    from bml.pd_d a
    left join tmp_mid_prod_inst_mp b on a.pd_inst_id = b.prod_id
    left join tmp_mid_prod_inst_main c on a.pd_inst_id = c.member_id
    left join tmp_mid_prod_inst_main_old h on a.pd_inst_id  = h.member_id
    left join tmp_mid_prod_inst_main_new i on a.pd_inst_id  = i.member_id
    left join tmp_mid_prod_inst_ehome d on a.pd_inst_id = d.member_id
    left join tmp_mid_prod_inst_nvgtn e on a.pd_inst_id = e.member_id
    left join tmp_mid_prod_inst_stdnt f on a.pd_inst_id = f.member_id
    left join tmp1_mid_prod_inst_vpn g on a.pd_inst_id = g.member_id

---------------------------------------procedure:SP_MID_PROD_INST--------------------------------------

  --21 mid_prod_inst_' || v_date_in ||
  create table mid_prod_inst_' || v_date_in || ' as
                   select a.PROD_ID,
                          nvl(a.ACCESS_NUMBER,a.ACC_NBR) ACCESS_NUMBER,
                          a.ACC_NBR   ,
                          a.PROD_SPEC_ID,
                          A.PROD_SPEC_NM,
                          a.PROD_TYPE ,
                          a.PROD_ADDR,
                          a.AREA_ID,
                          a.AREA_NAME,
                          a.sub_area_name  ,
                         a.sub_branch_name ,
                         a.FEE_AREA_NAME,
                         a.BRANCH_ID,
                         a.BRANCH_NAME,
                         a.DVLP_AREA_ID,
                         a.MANAGER_AREA_ID,
                         a.CSS_CUST_ID,
                         a.CUSTOM_TYPE_ID,
                         a.CUSTOM_TYPE_NAME,
                         a.CSS_CITY_ID,
                         a.DVLP_GRID_ID,
                         a.PHY_GRID_ID,
                         a.PHY_AREA_ID,
                         a.CSS_GRID_ID,
                         a.CSS_GRID_NAME,
                         a.CSS_MANAGER_ID,
                         a.CSS_MANAGER_NAME,
                         a.OWNER_ID,
                         a.OWNER_NAME,
                         a.ACCT_ID, 
                         a.IDENTITY_TYPE,
                         a.IDENTITY_NUM,
                         a.START_DT,
                         a.END_DT,
                         a.crtd_dt,
                         a.busi_st BSS_ST,
                         a.bss_st_name BSS_ST_NAME,
                         a.JF_STATE,
                         a.JF_STATE_NAME,
                         a.JF_STATE_DATE,
                         a.JF_CREATE_DATE,
                         a.min_owe_month OWE_MIN_MONTH,
                         a.owe_months,
                         a.owe_charge,
                         a.HK_FLAG ,
                         a.hk_IS_ACTIVE,
                         a.HK_STATE_DATE,
                         a.BILLING_MODE_ID,
                         a.BLNG_USER_F,
                         a.ONLINE_F,
                         a.CARD_3G_F,
                         a.GROUP_4G_F,
                         a.CHRG_F,
                         a.ON_INNET_LENGTH,
                         a.LAST_INNET_DT,
                         a.LAST_LV_DT,
                         a.sl_staff_id,
                         a.sl_staff_number,
                         a.SL_STAFF_NAME ,
                         a.XX_STAFF_ID ,
                         a.XX_STAFF_NUMBER,
                         a.XX_STAFF_NAME,
                         a.BSS_CHANNEL_ID,    
                         a.BSS_CHANNEL_NAME,
                         a.dvlp_agent_chnl_id,
                         a.AGENT_CHNL_ID,
                         a.AGENT_CHNL_NM,
                         a.KD_RAX,
                         A.BSS_KD_RAX,
                         a.ACC_TP,
                          CASE
                           WHEN A.ACC_TP IN (5, 6, 7, 9, 10)
                             and a.PROD_SPEC_ID in (9,10,11,12,13,14) THEN
                            '光接入'
                           WHEN A.ACC_TP not IN (5, 6, 7, 9, 10)
                             and a.PROD_SPEC_ID   in (9,10,11,12,13,14) THEN
                            '铜缆'
                            else null 
                         END ACCT_TP_NM,
                          T14.D_NM RMK, 
                         a.FQ_NAME,
                         a.NET_NBR IVPN_NBR,
                         a.IVPN_NAME,
                         c.LINK_NBR,
                         c.LINK_STAFF,
                         a.BASIC_OFFER_CODE,
                         a.BASIC_OFFER_NAME,  
                         b.o_pri_offer_id,
                         b.o_pri_offer_spec_id,
                         b.o_pri_offer_spec_name,  
                         b.o_pri_start_dt,
                         b.o_pri_end_dt,
                         b.o_pri_role_cd,
                         b.o_pri_status_cd,
                         b.PRI_OFFER_ID,
                         b.PRI_OFFER_SPEC_ID,
                         b.PRI_OFFER_SPEC_NAME,
                         b.PRI_START_DT,
                         b.PRI_END_DT,
                         b.PRI_ROLE_CD,
                         b.PRI_STATUS_CD,
                         b.n_pri_offer_id,
                         b.n_pri_offer_spec_id,
                         b.n_pri_offer_spec_name,
                         b.n_pri_start_dt,
                         b.n_pri_end_dt,
                         b.n_pri_role_cd,
                         b.n_pri_status_cd,
                         b.EHOME_PO_INST_ID,
                         b.EHOME_PO_SPEC_ID,
                         b.EHOME_PO_SPEC_NAME,
                         b.EHOME_PO_STATUS_CD,
                         b.EHOME_PO_SPEC_DT, 
                         b.NVGTN_PO_INST_ID,
                         b.NVGTN_PO_SPEC_ID,
                         b.NVGTN_PO_SPEC_NAME,
                         b.NVGTN_PO_STATUS_CD,
                         b.NVGTN_PO_SPEC_DT, 
                         b.E_STDNT_PO_INST_ID,
                         b.E_STDNT_PO_SPEC_ID,
                         b.E_STDNT_PO_SPEC_NAME,
                         b.E_STDNT_PO_STATUS_CD,
                         b.E_STDNT_PO_SPEC_DT, 
                         b.VPN_PO_INST_ID,
                         b.VPN_PO_SPEC_ID,
                         b.VPN_PO_SPEC_NAME,
                         b.VPN_PO_STATUS_CD,
                         b.VPN_PO_SPEC_DT,
                         sysdate UPDATE_DATE
  from tmp2_mid_prod_inst_hb a
  left join tmp3_mid_prod_inst_hb b
    on a.prod_id = b.pd_inst_id
  left join mid_prod_area c 
     on a.prod_id = c.prod_id
  LEFT JOIN BML.COMMON_D T14 --接入方式名称
    ON T14.D_CD = TO_CHAR(A.ACC_TP) AND T14.D_SCHM_ID = 212013

  --22 box_prod_info_temp
  truncate table etl_app.box_prod_info_temp

    --22.1 box_prod_info_temp
    insert into etl_app.box_prod_info_temp
    SELECT mpi.prod_id,
           mpi.fee_area_name,
           mpi.branch_name,
           mpi.css_grid_name,
           mpi.css_manager_name,
           mpi.pri_offer_spec_name,
           mpi.update_date
      FROM etl_app.mid_prod_inst mpi, etl_app.box_prod_spec ps
     WHERE mpi.prod_spec_id = ps.pd_spec_id
       and mpi.jf_state not like '%X';

    ALTER TABLE box_prod_info rename to box_prod_info_temp1;
    ALTER TABLE box_prod_info_temp rename to box_prod_info;
    ALTER TABLE box_prod_info_temp1 rename to box_prod_info_temp;
    