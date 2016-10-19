--本地加工过的中间表 **** 中间层的宽表
/*
产品中间表  MID_PROD_INST
销售品中间表_实例  MID_OFFER_INST
销售品中间表_成员  MID_OFFER_MEMBER_PROD
订单表（分竣工和全部）  MID_CO_BO_LIST
终端表  mid_trmnl_inst_first
收入中间表  mid_bil_acct_income
天翼宽表（日）  kb_scb_ty_day
宽带宽表（日）  mid_kd_scb
补贴中间表	etl_report.list_bt_offer
*/

select * from etl_app.mid_prod_inst_201609
select * from etl_app.mid_prod_inst--所有产品实例 --生产-包：etl_app.pkg_mid_prod_inst
select * from etl_app.MID_OFFER_INST where offer_id = '130591279031'
select * from etl_app.mid_offer_member_prod where offer_id = '130591279031' --所有销售品成员

select * from etl_app.mid_prod_cdma --cdma
select * from etl_app.mid_prod_kd --宽带

--从crm抽取的表:
select * from etl_load.bss_offer bo where bo.offer_id = '13000001859';--销售品实例表
select * from etl_load.bss_offer_spec bos where bos.offer_spec_id = '300006000153';--销售品规格表
select * from etl_load.bss_offer_prod;--产品实例
select * from etl_load.bss_offer_member;
select * from etl_load.bss_product where prod_id = '132021124039';

--从省公司下发的表：
select * from bml.pd_d

---生产报表的
select * from etl_report.



/*
 *
 *2016.10.14 政区V网取数
 *
 *
*/
select * from mid_prod_cdma_201609;--天翼宽表 chrg-f 出账标识   chrg_l1_after_tax出账税后金额
select * from mid_prod_cdma;
select * from mid_prod_inst where prod_id = '132021124039' ; -- IVPN
select * from mid_offer_inst a where a.offer_id = '130591278015';
select * from etl_app.mid_bt_list where prod_id = '132021124039' -- 补贴资费表 10 11 12 20 21 22    12正常 22 失效 10待生肖有未竣工   11将生效   20 待失效 21 将失效



select 
a.owner_name as 用户名,
a.access_number as 号码,
a.pri_offer_spec_name as 主套餐,
mbl.end_dt as 协议到期时间,
a.fee_area_name as 号码归属区域,
b.chrg_l1_after_tax as 九月出账情况,
c.chrg_l1_after_tax as 八月出账情况,
d.chrg_l1_after_tax as 七月出账情况
from mid_prod_inst a
join etl_app.mid_bt_list mbl on a.prod_id =  mbl.prod_id --补贴资费表
join mid_prod_cdma_201609 b on a.prod_id = b.prod_id
join mid_prod_cdma_201608 c on a.prod_id = c.prod_id
join mid_prod_cdma_201607 d on a.prod_id = d.prod_id
where a.ivpn_nbr = 'LNCVPN051322162011' --无索引 太慢 可建临时表
and mbl.offer_state = '12'; 

/*
    政企取电路信息 客户号 ：130000000066 --> css_cust_id
    电路包括DDN\FR\ATM\数字电路\数字中继
    
    电路包括A端B端 预后属性在产品实例表中有 资费属性在bss_offer_spec_param \ bss_offer_param \ item_spec_param中有(协议价等等)
*/
SELECT *
FROM MID_PROD_INST A
WHERE A.CSS_CUST_ID ='130000000066'
AND A.BLNG_USER_F='Y'
AND A.ACCESS_NUMBER LIKE 'M%' ;


select *
  from MID_OFFER_MEMBER_PROD B
 where b.member_id in (SELECT A.PROD_ID
                         FROM MID_PROD_INST A
                        WHERE A.CSS_CUST_ID = '130000000066'
                          AND A.BLNG_USER_F = 'Y'
                          AND A.ACCESS_NUMBER LIKE 'M%')

select 
a.css_cust_id as 客户id,
a.owner_name as 客户名称,
a.prod_spec_nm as 产品规格名称,
a.prod_type as 产品类型,
a.prod_addr as  安装地址,
a.start_dt as 产品生效时间,
a.kd_rax as 宽带速率
from 
MID_PROD_INST a

select --取电路及其资费
a.prod_id,
a.prod_spec_id,
a.access_number as 接入号,
a.css_cust_id as 客户id,
a.owner_name as 客户名称,
a.prod_spec_nm as 产品规格名称,
a.prod_type as 产品类型,
a.prod_addr as  安装地址,
a.start_dt as 产品生效时间,
a.kd_rax as 宽带速率,
m.value as 整条电路协议价每月单位元,
a.billing_mode_id as 预后
from 
MID_PROD_INST a
left join (
    select bop.offer_spec_param_id,t.member_id,t.offer_id,bis.item_spec_id,bis.name,bop.value
  from (select *
          from MID_OFFER_MEMBER_PROD B
         where b.member_id in
               (SELECT A.PROD_ID
                  FROM MID_PROD_INST A
                 WHERE A.CSS_CUST_ID = '130000000066'
                   AND A.BLNG_USER_F = 'Y'
                   AND A.ACCESS_NUMBER LIKE 'M%')) t
           join etl_load.bss_offer_param bop on t.offer_id = bop.offer_id
           join etl_load.bss_offer_spec_param bos on bos.offer_spec_param_id = bop.offer_spec_param_id
           join etl_load.bss_item_spec  bis on bis.item_spec_id = bos.item_spec_id
   where bis.item_spec_id = '100000' 
) m on a.prod_id = m.member_id
WHERE A.CSS_CUST_ID ='130000000066'
AND A.BLNG_USER_F='Y'
AND A.ACCESS_NUMBER LIKE 'M%'
and a.prod_spec_id = '' ;

/*
  电路产品
*/
select * from etl_load.bss_prod_spec where (name like '%电路%' or name like '%DDN%' or name like '%FR%' or name like '%ATM%' or name like '%中继%') 
