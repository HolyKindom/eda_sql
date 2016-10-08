--本地加工过的中间表 **** 中间层的宽表
select * from etl_app.mid_prod_inst_201609
select * from etl_app.mid_prod_inst--所有产品实例

select * from etl_app.mid_prod_cdma --cdma
select * from etl_app.mid_prod_kd --宽带






--从crm抽取的表:
select * from etl_load.bss_offer;--销售品实例表
select * from etl_load.bss_offer_spec bos where bos.offer_spec_id = '300509029691';--销售品规格表




--从省公司下发的表：
select * from bml.pd_d



---生产报表的
select * from etl_report.


