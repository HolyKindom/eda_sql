--���ؼӹ������м�� **** �м��Ŀ��
/*
��Ʒ�м��  MID_PROD_INST
����Ʒ�м��_ʵ��  MID_OFFER_INST
����Ʒ�м��_��Ա  MID_OFFER_MEMBER_PROD
�������ֿ�����ȫ����  MID_CO_BO_LIST
�ն˱�  mid_trmnl_inst_first
�����м��  mid_bil_acct_income
�������գ�  kb_scb_ty_day
�������գ�  mid_kd_scb
�����м��	etl_report.list_bt_offer
*/

select * from etl_app.mid_prod_inst_201609
select * from etl_app.mid_prod_inst--���в�Ʒʵ�� --����-����etl_app.pkg_mid_prod_inst
select * from etl_app.MID_OFFER_INST where offer_id = '130591279031'
select * from etl_app.mid_offer_member_prod where offer_id = '130591279031' --��������Ʒ��Ա

select * from etl_app.mid_prod_cdma --cdma
select * from etl_app.mid_prod_kd --���

--��crm��ȡ�ı�:
select * from etl_load.bss_offer bo where bo.offer_id = '13000001859';--����Ʒʵ����
select * from etl_load.bss_offer_spec bos where bos.offer_spec_id = '300006000153';--����Ʒ����
select * from etl_load.bss_offer_prod;--��Ʒʵ��
select * from etl_load.bss_offer_member;
select * from etl_load.bss_product where prod_id = '132021124039';

--��ʡ��˾�·��ı�
select * from bml.pd_d

---���������
select * from etl_report.



/*
 *
 *2016.10.14 ����V��ȡ��
 *
 *
*/
select * from mid_prod_cdma_201609;--������ chrg-f ���˱�ʶ   chrg_l1_after_tax����˰����
select * from mid_prod_cdma;
select * from mid_prod_inst where prod_id = '132021124039' ; -- IVPN
select * from mid_offer_inst a where a.offer_id = '130591278015';
select * from etl_app.mid_bt_list where prod_id = '132021124039' -- �����ʷѱ� 10 11 12 20 21 22    12���� 22 ʧЧ 10����Ф��δ����   11����Ч   20 ��ʧЧ 21 ��ʧЧ

select 
a.owner_name as �û���,
a.access_number as ����,
a.pri_offer_spec_name as ���ײ�,
mbl.end_dt as Э�鵽��ʱ��,
a.fee_area_name as �����������,
b.chrg_l1_after_tax as ���³������,
c.chrg_l1_after_tax as ���³������,
d.chrg_l1_after_tax as ���³������
from mid_prod_inst a
join etl_app.mid_bt_list mbl on a.prod_id =  mbl.prod_id --�����ʷѱ�
join mid_prod_cdma_201609 b on a.prod_id = b.prod_id
join mid_prod_cdma_201608 c on a.prod_id = c.prod_id
join mid_prod_cdma_201607 d on a.prod_id = d.prod_id
where a.ivpn_nbr = 'LNCVPN051322162011' --������ ̫�� �ɽ���ʱ��
and mbl.offer_state = '12'; 


