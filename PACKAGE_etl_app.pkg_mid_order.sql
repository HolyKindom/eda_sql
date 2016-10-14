/*
	PACKAGE BODY ETL_APP.PKG_MID_ORDER

	contains:
	1) SP_MID_ORDER_LIST 购物车清单
	2) SP_MID_BO_LIST 受理动作清单
	3) SP_CFG_ZX_CHNEL_STAFF 
	4) SP_MID_PROD_AREA 产品区域
	5) SP_MID_OFFER_AREA 销售品区域

*/
------------------------------------------PROCEDURE SP_MID_ORDER_LIST----------------------------------------------
/*
 目标表：MID_ORDER_LIST
*/

-- MID_ORDER_LIST_DAY
TRUNCATE TABLE MID_ORDER_LIST_DAY;

INSERT /*+APPEND NOLOGGING*/ INTO MID_ORDER_LIST_DAY
        (GENERATE_DATE,
         OL_ID,
         OL_NBR,
         STAFF_ID,
         STAFF_NUM,
         STAFF_NAME,
         CHANNEL_ID,
         CHANNEL_NAME,
         CHANNEL_AREA_NAME,
         AREA_ID,
         AREA_NAME,
         SO_DATE,
         STATUS_CD,
         STATUS_NAME,
         STATUS_DT,
         CHANNEL_SPEC_ID,
         CHANNEL_SPEC_NAME)
            SELECT TO_DATE(' || V_DATE ||
             ',''YYYYMMDD''),
               A.OL_ID,
               A.OL_NBR,
               (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN AC.STAFF_ID ELSE  A.STAFF_ID END) STAFF_ID,
               (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN B1.STAFF_NUMBER ELSE  B.STAFF_NUMBER END) STAFF_NUMBER,
               (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN B1.NAME ELSE  B.NAME END) STAFF_NAME,
               (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN AC.CHANNEL_ID ELSE A.CHANNEL_ID END) CHANNEL_ID ,
               (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN F1.NAME ELSE F.NAME END) CHANNEL_NAME ,             
               (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN G1.NAME ELSE G.NAME END ) CHANNEL_AREA_NAME,
               A.AREA_ID,
               D.NAME            AREA_NAME,
               A.SO_DATE,
               A.STATUS_CD,
               E.NAME            STATUS_NAME,
               A.STATUS_DT,
              (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN F1.CHANNEL_SPEC_ID ELSE F.CHANNEL_SPEC_ID END) CHANNEL_SPEC_ID ,    
              (CASE WHEN AO.ACCEPT_ORDER_ID IS NOT NULL AND AC.ACCEPT_ORDER_ID IS NOT NULL THEN H1.NAME ELSE H.NAME END) CHANNEL_SPEC_NAME
          FROM ETL_LOAD.BSS_ORDER_LIST A
          LEFT JOIN ETL_LOAD.BSS_OUR_STAFF B
            ON A.STAFF_ID = B.STAFF_ID
          LEFT JOIN ETL_LOAD.BSS_AREA D
            ON A.AREA_ID = D.AREA_ID
          LEFT JOIN ETL_LOAD.BSS_ORDER_STATUS E
            ON A.STATUS_CD = E.STATUS_CD
            
          LEFT JOIN ETL_LOAD.BSS_CHANNEL F
            ON A.CHANNEL_ID = F.CHANNEL_ID
          LEFT JOIN ETL_LOAD.BSS_AREA G
            ON F.AREA_ID = G.AREA_ID
          LEFT JOIN ETL_LOAD.BSS_CHANNEL_SPEC H
            ON F.CHANNEL_SPEC_ID = H.CHANNEL_SPEC_ID
            
          LEFT JOIN ETL_LOAD.BSS_ACCEPT_ORDER_2_STEP_OLID AO 
            ON A.OL_NBR = AO.OL_NBR --甩单意向单
          LEFT JOIN ETL_LOAD.BSS_ACCEPT_ORDER AC 
            ON AO.ACCEPT_ORDER_ID = AC.ACCEPT_ORDER_ID
          LEFT JOIN ETL_LOAD.BSS_OUR_STAFF B1 
            ON AC.STAFF_ID = B1.STAFF_ID
          LEFT JOIN ETL_LOAD.BSS_CHANNEL F1 
            ON AC.CHANNEL_ID = F1.CHANNEL_ID
          LEFT JOIN ETL_LOAD.BSS_AREA G1 
            ON F1.AREA_ID = G1.AREA_ID
          LEFT JOIN ETL_LOAD.BSS_CHANNEL_SPEC H1
            ON F1.CHANNEL_SPEC_ID = H1.CHANNEL_SPEC_ID
         WHERE A.STATUS_DT >= TO_DATE(' || V_DATE ||', ''YYYYMMDD'');

DELETE FROM  MID_ORDER_LIST A
	WHERE A.GENERATE_DATE = TO_DATE(' || V_DATE || ',''YYYYMMDD'');

DELETE FROM  MID_ORDER_LIST A
	WHERE A.OL_ID IN (SELECT OL_ID FROM MID_ORDER_LIST_DAY);

--MID_ORDER_LIST
INSERT /*+APPEND NOLOGGING*/ INTO MID_ORDER_LIST
  SELECT * FROM MID_ORDER_LIST_DAY A;


------------------------------------------PROCEDURE SP_MID_BO_LIST----------------------------------------------
/*
	源表：
	所需源表：
    ETL_APP.MID_ORDER_LIST ;      ---生成
    ETL_APP.CFG_PROD_SPEC_TYPE;     ---死表没有更新维护
    ETL_LOAD.BSS_BUSI_ORDER;
    ETL_LOAD.BSS_OFFER_ORDER;
    ETL_LOAD.BSS_OFFER_SPEC;
    ETL_LOAD.BSS_BO_ACTION_TYPE;
    ETL_LOAD.BSS_PROD_ORDER;
    ETL_LOAD.BSS_BO_SERV_ORDER;
    ETL_LOAD.BSS_SERV_SPEC;
    ETL_LOAD.BSS_ACCT_INFO_ORDER;
    ETL_LOAD.BSS_CUST_INFO_ORDER;
    ETL_LOAD.BSS_OUR_STAFF;
    ETL_LOAD.BSS_ORDER_STATUS;
    ETL_LOAD.BSS_OO_ROLE;
    ETL_LOAD.BSS_BO_SERV;
    ETL_LOAD.BSS_BO_PROD_ADDRESS;
    ETL_LOAD.BSS_OO_OWNER;
    ETL_LOAD.BSS_PARTY;
    ETL_LOAD.BSS_PARTY_SEGMENT_MEMBER_LIST;
    ETL_LOAD.BSS_SEGMENT;
    ETL_LOAD.BSS_BO_PROD_SPEC;
    ETL_LOAD.BSS_BO_CUST;
    ETL_LOAD.BSS_BO_PROD_2_TD;
    ETL_LOAD.BSS_PROD_SPEC;
    ETL_LOAD.BSS_OFFER_PROD_2_ADDR;
    ETL_LOAD.BSS_OFFER_PROD;
*/

-- TMP_MID_BO_LIST_ALL
   ------取工单的基本信息，产品，销售品，服务，客户，合同号
CREATE TABLE TMP_MID_BO_LIST_ALL AS
SELECT TO_DATE(' || V_DATE || ', ''YYYYMMDD'') GENERATE_DATE,
       ''' || V_DATE1 || ''' GENERATE_DT,
       A.OL_ID OL_ID,
       OL.OL_NBR OL_NBR,
       A.SEQ SEQ,
       A.STAFF_ID STAFF_ID,
       OST. STAFF_NUMBER,
       OST.NAME STAFF_NAME,
       A.STATUS_CD STATUS_CD,
       OS.NAME STATUS_CD_NAME,
       A.STATUS_DT STATUS_DT,
       A.ARCHIVE_DT ARCHIVE_DT,
       A.COMPLETE_DT COMPLETE_DT,
       A.BO_ID BO_ID,
       A.BO_ACTION_TYPE_CD BO_ACTION_TYPE_CD,
       D.NAME BO_ACTION_TYPE_NAME,
       B.OFFER_SPEC_ID OFFER_SPEC_ID,
       C.AGREEMENT_TYPE_CD AGREEMENT_TYPE_CD,
       C.NAME OFFER_SPEC_NAME,
       C.OFFER_TYPE_CD OFFER_TYPE_CD,
       B.OFFER_ID OFFER_ID,
       B.OFFER_NBR OFFER_NBR,
       B.STATE OFFER_STATE,
       B.ATOM_ACTION_ID OFFER_ATOM_ACTION_ID,
       E.PROD_ID PROD_ID,
       E.STATE PROD_STATE,
       E.ATOM_ACTION_ID PROD_ATOM_ACTION_ID,
       F1.SERV_ID SERV_ID,
       F1.SERV_SPEC_ID SERV_SPEC_ID,
       F11.NAME SERV_SPEC_NAME,
       F2.ACCT_ID ACCT_ID,
       F2.STATE ACCT_STATE,
       F2.ATOM_ACTION_ID ACCT_ATOM_ACTION_ID,
       F3.PARTY_ID PARTY_ID,
       F3.STATE CUST_STATE,
       F3.ATOM_ACTION_ID CUST_ATOM_ACTION_ID,
       CASE
         WHEN B.OFFER_ID IS NOT NULL THEN
          ''OFFER''
         WHEN E.PROD_ID IS NOT NULL AND F1.SERV_ID IS NULL THEN
          ''PROD''
         WHEN E.PROD_ID IS NOT NULL AND F1.SERV_ID IS NOT NULL THEN
          ''SERV''
         WHEN F2.ACCT_ID IS NOT NULL THEN
          ''ACCT''
         WHEN F3.PARTY_ID IS NOT NULL THEN
          ''CUST''
         ELSE
          ''OTHER''
       END BO_FLAG,
       A.PARTY_ID BO_PARTY_ID,
       OL.SO_DATE SO_DATE,
       OL.AREA_ID AREA_ID,
       OL.CHANNEL_ID CHANNEL_ID,
       OL.CHANNEL_NAME CHANNEL_NAME,
       OL.CHANNEL_AREA_NAME CHANNEL_AREA_NAME,
       OL.STAFF_ID SS_STAFF_ID,
       OL.STAFF_NUM SS_STAFF_NUMBER,
       OL.STAFF_NAME SS_STAFF_NAME
  FROM ETL_LOAD.BSS_BUSI_ORDER A
  LEFT JOIN ETL_APP.MID_ORDER_LIST OL ON A.OL_ID = OL.OL_ID
  LEFT JOIN ETL_LOAD.BSS_OFFER_ORDER B ON A.BO_ID = B.BO_ID  
  LEFT JOIN ETL_LOAD.BSS_OFFER_SPEC C ON B.OFFER_SPEC_ID = C.OFFER_SPEC_ID
  LEFT JOIN ETL_LOAD.BSS_BO_ACTION_TYPE D ON A.BO_ACTION_TYPE_CD =  D.BO_ACTION_TYPE_CD
  LEFT JOIN ETL_LOAD.BSS_PROD_ORDER E ON A.BO_ID = E.BO_ID 
  LEFT JOIN ETL_LOAD.BSS_BO_SERV_ORDER F1 ON A.BO_ID = F1.BO_ID 
  LEFT JOIN ETL_LOAD.BSS_SERV_SPEC F11 ON F1.SERV_SPEC_ID =  F11.SERV_SPEC_ID
  LEFT JOIN ETL_LOAD.BSS_ACCT_INFO_ORDER F2 ON A.BO_ID = F2.BO_ID  
  LEFT JOIN ETL_LOAD.BSS_CUST_INFO_ORDER F3 ON A.BO_ID = F3.BO_ID  
  LEFT JOIN ETL_LOAD.BSS_OUR_STAFF OST ON A.STAFF_ID = OST.STAFF_ID
  LEFT JOIN ETL_LOAD.BSS_ORDER_STATUS OS ON A.STATUS_CD =  OS.STATUS_CD;

-- TMP_MID_BO_LIST_PROD ---取销售品对应的产品ID，多个产品的规则需要确认。产品号码，最后打REDU_ACCESS_NUMBER
CREATE TABLE TMP_MID_BO_LIST_PROD AS    
 SELECT A.BO_ID,
        B.PROD_ID,
        B.OBJ_TYPE,
        B.OBJ_INST_ID,
        ROW_NUMBER() OVER(PARTITION BY B.BO_ID ORDER BY A.PROD_ID NULLS LAST) REQ
   FROM TMP_MID_BO_LIST_ALL A
  INNER JOIN ETL_LOAD.BSS_OO_ROLE B ON A.BO_ID = B.BO_ID;

CREATE TABLE TMP_MID_BO_LIST_PROD1 AS
SELECT AA.BO_ID,AA.PROD_ID,AA.OBJ_TYPE,AA.OBJ_INST_ID SUB_OFFER_ID
  FROM (SELECT A.*,
               ROW_NUMBER() OVER(PARTITION BY A.BO_ID ORDER BY A.PROD_ID NULLS LAST) RN
          FROM TMP_MID_BO_LIST_PROD A
         WHERE A.OBJ_TYPE = 7) AA
 WHERE RN = 1;

-- TMP_MID_BO_LIST_SERV ---取服务状态
CREATE TABLE TMP_MID_BO_LIST_SERV AS
SELECT T1.BO_ID, T2.STATE SERV_SPEC_STATE,
       ROW_NUMBER() OVER(PARTITION BY T1.BO_ID ORDER BY T2.END_DT) REQ
  FROM TMP_MID_BO_LIST_ALL T1
 INNER JOIN ETL_LOAD.BSS_BO_SERV T2 ON T1.BO_ID = T2.BO_ID;

-- TMP_MID_BO_LIST_ADDR --装机地址,还有一部分未打上的下面根据产品ID打
CREATE TABLE TMP_MID_BO_LIST_ADDR AS
SELECT T1.BO_ID, T2.ADDR_STD PROD_ADDR_STD,
       ROW_NUMBER() OVER(PARTITION BY T1.BO_ID ORDER BY T2.ATOM_ACTION_ID DESC) REQ
  FROM TMP_MID_BO_LIST_ALL T1
 INNER JOIN ETL_LOAD.BSS_BO_PROD_ADDRESS T2 ON T1.BO_ID = T2.BO_ID
 WHERE T2.STATE IN (''ADD'', ''KIP'', ''NEW'')

-- TMP_MID_BO_LIST_PARTY 产品拥有者,两个有区别需要确认
CREATE TABLE TMP_MID_BO_LIST_PARTY AS
SELECT A.BO_ID,
       B.PARTY_ID PROD_PARTY_ID,
       C.NAME PROD_PARTY_NAME,
       C.ADDRESS_STR PROD_PARTY_ADDR,
       ROW_NUMBER() OVER(PARTITION BY B.BO_ID ORDER BY B.ATOM_ACTION_ID DESC) REQ
  FROM TMP_MID_BO_LIST_ALL A
 INNER JOIN ETL_LOAD.BSS_OO_OWNER B ON A.BO_ID = B.BO_ID
  LEFT JOIN ETL_LOAD.BSS_PARTY C ON B.PARTY_ID = C.PARTY_ID

------------------------------------------PROCEDURE SP_CFG_ZX_CHNEL_STAFF----------------------------------------------
------------------------------------------PROCEDURE SP_MID_PROD_AREA----------------------------------------------
------------------------------------------PROCEDURE SP_MID_OFFER_AREA----------------------------------------------