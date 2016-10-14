/**
    -----------------------程序描述--------------------------------
    程序功能：销售品配置表
    -------------------------参数说明--------------------------------
  所需源表
  merge_offer_type   ---死表
  CFG_OFFER_CDMA_DCP   ---死表
  CFG_OFFER_CDMA_RH    ---死表
  cfg_prid_kd_seq    ---死表
  
  ETL_LOAD.BSS_OFFER_ROLES    ----本过程更新
  
  etl_load.bss_offer_spec
  etl_load.bss_member_role
  BML.PO_SPEC_D
  BML.PO_SPEC_CATE_TREE
  
  生成的表
  cfg_offer_spec
  cfg_offer_role
  ETL_LOAD.BSS_OFFER_ROLES
  **/

--1 etl_load.bss_offer_roles ETL_LOAD.BSS_OFFER_ROLES
  --1.1
  truncate table ETL_LOAD.BSS_OFFER_ROLES;
  --1.2
  insert into ETL_LOAD.BSS_OFFER_ROLES
    (OFFER_ROLE_ID,
     OFFER_SPEC_ID,
     NAME,
     CODE,
     ROLE_CD,
     IF_HARDCORE,
     IS_VISIBLE,
     DEFAULT_QTY,
     LEAST_QTY,
     MIN_QTY,
     MAX_QTY,
     QTY_RULE,
     COMP_ORDER,
     START_DT,
     END_DT,
     CREATE_DT,
     VERSION)
    select * from   etl_load.BSS_OFFER_ROLES_TEMP;

--2 cfg_offer_spec
  --2.1
  truncate table cfg_offer_spec
  --2.2
  insert into cfg_offer_spec
  (OFFER_ID,
   OFFER_SPEC_ID,
   OFFER_SPEC_NAME,
   OFFER_TYPE_CD,
   cate_nm_1,
   cate_nm_2,
   cate_nm_3,
   cate_nm_4,
   cate_nm_5,
   cate_nm_6,
   cate_nm_7,
   limit_mon_chrg,
   eff_time,
   OFFER_NAME,
   OFFER_TYPE,
   MERGE_TYPE,
   MERGE_TYPE1,
   MERGE_TYPE2,
   MERGE_TYPE3)
  SELECT c.code,
         c.offer_spec_id,
         c.name offer_spec_name,
         c.offer_type_cd,
         cate_nm_1,
         cate_nm_2,
         cate_nm_3,
         cate_nm_4,
         cate_nm_5,
         cate_nm_6,
         cate_nm_7,
         limit_mon_chrg,
         eff_time,
         d.OFFER_NAME,
         d.OFFER_TYPE,
         trim(d.MERGE_TYPE),
         trim(d.MERGE_TYPE1),
         trim(d.MERGE_TYPE2),
         trim(d.MERGE_TYPE3)
    FROM etl_load.bss_offer_spec c
    LEFT JOIN BML.PO_SPEC_D B on B.PO_SPEC_ID = c.code
    LEFT JOIN BML.PO_SPEC_CATE_TREE A ON A.PO_SPEC_CATE_ID =
                                         B.DFLT_PO_SPEC_CATE_ID
                                     and b.city_id IN (''E'', ''Z'', ''A'')
                                     and cate_nm_1 in
                                         (''2组合产品'', ''3单产品主套餐'', ''4叠加资费'', ''5增值'',
                                          ''6号百'', ''7ICT(行业应用)'')
    left join merge_offer_type d on c.code = d.offer_id

--3 cfg_offer_role
  --3.1
  truncate table cfg_offer_role
  --3.2
  insert into cfg_offer_role
  select a.*, b.offer_role_id, c.role_cd, c.name role_name, ''''
    from cfg_offer_spec           a,
         etl_load.bss_offer_roles b,
         etl_load.bss_member_role c
   where a.offer_spec_id = b.offer_spec_id
     and b.role_cd = c.role_cd
  --3.3
  update cfg_offer_role t
   set t.role_desc = 2
 where role_name like ''%加装%''
    or role_name like ''%可选%''
    or role_name like ''%捆绑电话%''
    or role_name like ''%基础固话1%''
    or role_name like ''%基础固话2%''
    or role_name like ''%基础固话3%''
    or role_name like ''%基础固话4%''
    or role_name like ''%e家基础共享C网成员%'';

  update cfg_offer_role t
   set t.role_desc = 3
 where  role_cd = 330;

 update cfg_offer_role t set t.role_desc = 1 where role_desc is null;



--4 ETL_LOAD.BSS_OFFER_ROLES
  update ETL_LOAD.BSS_OFFER_ROLES a
    set (a.OFFER_TYPE_CD, a.OFFER_SPEC_NAME) =
       (select b.offer_type_cd, b.name
          from ETL_LOAD.BSS_OFFER_SPEC b
         where a.offer_spec_id = b.offer_spec_id)

  update ETL_LOAD.BSS_OFFER_ROLES a
    set a.ROLE_CD_NAME =
       (select b.name
          from etl_load.bss_member_role b
         where a.role_cd = b.role_cd)

  update ETL_LOAD.BSS_OFFER_ROLES a
    set (a.RULE_DESC,
        a.MERGE_TYPE,
        a.MERGE_TYPE1,
        MERGE_TYPE2,
        a.cate_nm_1,
        a.cate_nm_2,
        a.cate_nm_3,
        a.cate_nm_4,
        a.cate_nm_5,
        a.cate_nm_6,
        a.cate_nm_7) =
       (select distinct b.role_desc,
                        b.merge_type,
                        b.merge_type1,
                        b.merge_type2,
                        b.cate_nm_1,
                        b.cate_nm_2,
                        b.cate_nm_3,
                        b.cate_nm_4,
                        b.cate_nm_5,
                        b.cate_nm_6,
                        b.cate_nm_7
          from cfg_offer_role b
         where a.offer_role_id = b.offer_role_id)

         
  update ETL_LOAD.BSS_OFFER_ROLES a  --天翼主资费修改为新的模式
    set a.main_offer_seq = 1
   where exists (select 1
            from CFG_OFFER_CDMA_DCP
           where offer_spec_id = a.offer_spec_id)    

  update ETL_LOAD.BSS_OFFER_ROLES a  
    set a.main_offer_seq = 2
   where exists (select 1
                from CFG_OFFER_CDMA_RH
               where offer_spec_id = a.offer_spec_id)
         and main_offer_seq is null

  update ETL_LOAD.BSS_OFFER_ROLES a  --20150615jjj新增移动品牌资费
    set a.main_offer_seq = 10
   where a.offer_spec_id in (300500003485, 300500003058)

  update ETL_LOAD.BSS_OFFER_ROLES a  --宽带主资费更新
   set A.MAIN_OFFER_SEQ =
       (select yxjb
          from etl_load.cfg_prid_kd_seq
         where offer_role_id = a.offer_role_id)
 where offer_role_id in
       (select offer_role_id from etl_load.cfg_prid_kd_seq)

  update ETL_LOAD.BSS_OFFER_ROLES a  --非天翼和宽带成员,属于融合套餐的成员
   set A.MAIN_OFFER_SEQ = 99
 where a.cate_nm_2 in ('' 我的E家 '', '' 商务领航 '')
   and MAIN_OFFER_SEQ is null

  update ETL_LOAD.BSS_OFFER_ROLES a
    set A.MAIN_OFFER_SEQ = 100
  WHERE MAIN_OFFER_SEQ IS NULL