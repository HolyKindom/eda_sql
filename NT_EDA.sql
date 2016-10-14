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


