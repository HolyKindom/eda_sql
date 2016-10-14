select name,locks,pins
from v$db_object_cache
where locks > 0 and pins > 0 and type='PROCEDURE';
/*NAME                  LOCKS     PINS
P_ETL_CRM_DESK     1        1*/

--2.查询v$open_cursor 视图
select sid,sql_text
from v$open_cursor
where UPPER(sql_text) like '%P_ETL_CRM_DESK%'
SID     SQL_TEXT
143     begin   -- Call the procedure   p_etl_crm_desk(v_dtdate => :

--3.也可以用v$access确定
select * from v$access where object='P_ETL_CRM_DESK';
--SID     OWNER     OBJECT                    TYPE
--143     KDCC     P_ETL_CRM_DESK     PROCEDURE

--4.或者dba_ddl_locks
select session_id sid, owner, name, type,mode_held held, mode_requested request
from dba_ddl_locks
where name = 'P_ETL_CRM_DESK';
/*SID     OWNER     NAME                             TYPE                   HELD     REQUEST
143     KDCC     P_ETL_CRM_DESK     Table/Procedure/Type     Null        None*/


select * from all_objects ao where ao.OBJECT_TYPE
select distinct(uo.OBJECT_TYPE)  from user_objects uo

select * from user_objects uo where uo.OBJECT_TYPE = 'PROCEDURE' and uo.OBJECT_NAME like '%SP_MID%'
select * from user_objects uo where uo.OBJECT_TYPE = 'TABLE' and uo.OBJECT_NAME like '%MID%'
select * from user_objects uo where uo.OBJECT_TYPE = 'PROCEDURE' and uo.OBJECT_NAME like '%SP_CFG%'


select * from all_objects ao where ao.OBJECT_NAME like '%SP_MID_PROD%';
select * from all_objects ao where ao.OBJECT_NAME like '%SP_MID%OFFER%';
INST_OFFER






















