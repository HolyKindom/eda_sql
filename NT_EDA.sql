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

/*
    ����ȡ��·��Ϣ �ͻ��� ��130000000066 --> css_cust_id
    ��·����DDN\FR\ATM\���ֵ�·\�����м�
    
    ��·����A��B�� Ԥ�������ڲ�Ʒʵ�������� �ʷ�������bss_offer_spec_param \ bss_offer_param \ item_spec_param����(Э��۵ȵ�)
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
a.css_cust_id as �ͻ�id,
a.owner_name as �ͻ�����,
a.prod_spec_nm as ��Ʒ�������,
a.prod_type as ��Ʒ����,
a.prod_addr as  ��װ��ַ,
a.start_dt as ��Ʒ��Чʱ��,
a.kd_rax as �������
from 
MID_PROD_INST a

select --ȡ��·�����ʷ�
a.prod_id,
a.prod_spec_id,
a.access_number as �����,
a.css_cust_id as �ͻ�id,
a.owner_name as �ͻ�����,
a.prod_spec_nm as ��Ʒ�������,
a.prod_type as ��Ʒ����,
a.prod_addr as  ��װ��ַ,
a.start_dt as ��Ʒ��Чʱ��,
a.kd_rax as �������,
m.value as ������·Э���ÿ�µ�λԪ,
a.billing_mode_id as Ԥ��
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
  ��·��Ʒ
*/
select * from etl_load.bss_prod_spec where (name like '%��·%' or name like '%DDN%' or name like '%FR%' or name like '%ATM%' or name like '%�м�%') 
