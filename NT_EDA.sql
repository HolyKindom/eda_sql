--NT_EDA/nteda_519@NTDM_DM1
--etl_cbzt/cbzt_0116@ntsjjs

select * from dual

--本地加工过的中间表 **** 中间层的宽表
select * from etl_app.mid_prod_inst_201609
select * from etl_app.mid_prod_inst--所有产品实例

select * from etl_app.mid_prod_cdma --cdma
select * from etl_app.mid_prod_kd --宽带






--从crm抽取的表:
select * from etl_load.b




--从省公司下发的表：
select * from bml.pd_d



---生产报表的
select * from etl_report.


