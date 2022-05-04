--RentSmart Source Query:
--SAM SOURCE (EDW)
	--QUERY
SELECT 
       [COB_SAM_ID]
      ,[COB_SAM_ADDRESS_TYPE]
      ,substring(COB_SAM_FULL_ADDRESS, 1, 255) as COB_SAM_FULL_ADDRESS
      ,[COB_SAM_STREET_NUMBER]
      ,[COB_SAM_FULL_STREET_NAME]
      ,[COB_SAM_UNIT_NUMBER]
      ,[COB_SAM_ZIP_CODE]
      ,[COB_SAM_MAILING_NEIGHBORHOOD]
      ,[COB_SAM_BUILDING_ID]
      ,[COB_SAM_LONG]
      ,[COB_SAM_LAT]
      ,[COB_SAM_PID]
FROM [COB_SAM_DIM]
WHERE ETL_LOAD_ACT_FLAG = 'Y';
--388056 records
--LOAD TIMES: 577 seconds

	--DDL
CREATE TABLE boston.rs_sam_source
(
	cob_sam_id character varying(50) NOT NULL,
	cob_sam_address_type character varying(100),
	cob_sam_full_address character varying(255),
	cob_sam_street_number character varying(21),
	cob_sam_full_street_name character varying(255),
	cob_sam_unit_number character varying(15),
	cob_sam_zip_code character varying(5),
	cob_sam_mailing_neighborhood character varying(50),
	cob_sam_building_id int,
	cob_sam_long decimal (19, 9),
	cob_sam_lat decimal (19, 9),
	cob_sam_pid character varying(100),
	cob_sam_etl_load_date date,
  CONSTRAINT "rs_sam_source_pk" PRIMARY KEY (cob_sam_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_sam_source
  OWNER TO u9as36ung5msfk;

--ASSESSING SOURCE (EDWS)
	--QUERY
	SELECT 
       [AST_PID]
      ,[AST_CM_ID]
      ,[AST_ST_NUM]
      ,[AST_ST_NAME]
      ,[AST_ST_NAME_SUF]
      ,[AST_UNIT_NUM]
      ,[AST_ZIPCODE]
      ,[AST_PTYPE]
      ,[AST_LU]
      ,[AST_OWNER]
      ,[AST_YR_BUILT]
      ,[AST_YR_REMOD]
      ,[AST_LIVING_AREA]
      ,[AST_R_BDRMS]
      ,[AST_R_FULL_BTH]
      ,[AST_R_HALF_BTH]
      ,[AST_U_BDRMS]
      ,[AST_U_FULL_BTH]
      ,[AST_U_HALF_BTH]
  FROM [AST_ASSESSMENT_SNAPSHOT_STAGE]
  where AST_YEAR = '2016'
  and AST_LU IN ('A', 'CD', 'CM', 'E', 'EA', 'R1', 'R2', 'R3', 'R4', 'RC' )

	--146845
	--DDL
CREATE TABLE boston.rs_ast_source
(

	ast_pid character varying(50) NOT NULL,
	ast_cm_id character varying(50),
	ast_st_num character varying(50),
	ast_st_name character varying(50),
	ast_st_name_suf character varying(50),
	ast_unit_num character varying(50),
	ast_zipcode character varying(50),
	ast_ptype int,
	ast_lu character varying(50),
	ast_owner character varying(50),
	ast_yr_built character varying(50),
	ast_yr_remod int,
	ast_living_area bigint,
	ast_r_bdrms bigint,
	ast_r_full_bth bigint,
	ast_r_half_bth bigint,
	ast_u_bdrms character varying(50),
	ast_u_full_bth character varying(50),
	ast_u_half_bth character varying(50),
	ast_etl_load_date date,
  CONSTRAINT "rs_ast_source_pk" PRIMARY KEY (ast_pid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_ast_source
  OWNER TO u9as36ung5msfk;

--311 LAGAN SOURCE (EDWS from...?)
	--QUERY (STAGING TO EDWS)
SELECT distinct 
     [CASE_ENQUIRY_ID]
      ,[OPEN_DT]
      ,[REASON]
      ,[TYPE]
      ,isnull(substring([propId],2,10),'') as [samid]
 FROM [LaganPDM].[dbo].[lgnbo_CASE_ENQUIRY_B00_DENYHS]
 WHERE case_title not like 'Migrated%'
and OPEN_DT >= '7/1/2011'
and QUEUE not like 'Z_do%'
AND TYPE not in
('911'
,'General Request'
,'Special Request')
and subject != 'Boston Public School'
and isnull(substring([propId],1,1),'')  = 'A'
and TYPE not like '%Snow%'
and REASON in (
'Abandoned Bicycle',--sanitation
'Graffiti',
'Street Cleaning',
'Enforcement & Abandoned Vehicles',
'Environmental Services',
'Highway Maintenance',
'Air Pollution Control',--housing
'Housing',
'Building',
'Catchbasin',--civic
'Pothole',
'Sidewalk Cover / Manhole',
'Signs & Signals',
'Street Lights',
'Trees',
'Water Issues'
--'Highway Maintenance'
);
	--DDL
CREATE TABLE boston.rs_mhl_source
(

	mhl_case_id numeric (18,0) NOT NULL,
	mhl_open_dt date,
	mhl_reason character varying(50),
	mhl_type character varying(50),
	mhl_sam_id character varying(50),
	mhl_etl_load_date date,
  CONSTRAINT "rs_mhl_source_pk" PRIMARY KEY (mhl_case_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_mhl_source
  OWNER TO u9as36ung5msfk;

--ISD HANSEN SOURCE (EDW)
	--QUERY

SELECT * FROM
(
SELECT
  Table__16."HAN_CAS_AP_NO" as ticketno,
  convert(date, Table__20."HAN_INS_RESULT_DTTM") as event_date,
  Table__16."HAN_CAS_STATUS" as status,
  case when Table__37."LKP_INS_DESCRIPT" is null then Table__38."LKP_CAS_DESCRIPT" else Table__37."LKP_INS_DESCRIPT" end as description,
  Table__14."HAN_ADD_BLOCK" as samid
FROM
  "dbo"."HAN_ADDRESS_DIM"  Table__14 INNER JOIN "dbo"."HAN_INSPECTION_FACT"  Table__20 ON (Table__20."HAN_INS_ADD_DIM_KEY"=Table__14."HAN_ADD_DIM_KEY")
   INNER JOIN "dbo"."HAN_CASE_DIM"  Table__16 ON (Table__20."HAN_INS_CAS_DIM_KEY"=Table__16."HAN_CAS_DIM_KEY")
   LEFT OUTER JOIN "dbo"."HAN_CASE_LKP"  Table__38 ON (Table__16."HAN_CAS_KEY"=Table__38."LKP_CAS_KEY")
   INNER JOIN "dbo"."HAN_RESOURCE_DIM"  Table__23 ON (Table__20."HAN_INS_PRI_APL_DIM_KEY"=Table__23."HAN_RES_DIM_KEY")
   LEFT OUTER JOIN "dbo"."HAN_INSPECTION_LKP"  Table__37 ON (Table__20."HAN_INS_FACT_KEY"=Table__37."LKP_INS_APCASEINSPKEY")  
WHERE
  (
   Table__16."HAN_CAS_TYPE_DESC"  =  'Code Enforcement'
   AND
   Table__23."HAN_RES_DIM_KEY"  NOT IN  ( 1,2  )
   AND
   Table__20."HAN_INS_RESULT_DTTM"  <  convert(date,getdate())
   AND
   Table__16."HAN_CAS_STATUS"  <>  'Deleted'
   AND
   ( Table__20.ETL_LOAD_SOURCE = 'CASEINSP'  )
   AND
   Table__14."HAN_ADD_BLOCK"  Is Not Null  
  )
GROUP BY
  Table__16."HAN_CAS_AP_NO", 
  convert(date, Table__20."HAN_INS_RESULT_DTTM"), 
  Table__16."HAN_CAS_STATUS", 
  case when Table__37."LKP_INS_DESCRIPT" is null then Table__38."LKP_CAS_DESCRIPT" else Table__37."LKP_INS_DESCRIPT" end, 
  Table__14."HAN_ADD_BLOCK"
UNION  
SELECT
  Table__16."HAN_CAS_AP_NO" as ticketno,
  convert(date, Table__20."HAN_INS_RESULT_DTTM") as event_date,
  Table__16."HAN_CAS_STATUS" as status,
  case when Table__38."LKP_CAS_DESCRIPT" is null then Table__37."LKP_INS_DESCRIPT" else Table__38."LKP_CAS_DESCRIPT" end as description,
  Table__14."HAN_ADD_BLOCK" as samid
FROM
  "dbo"."HAN_ADDRESS_DIM"  Table__14 INNER JOIN "dbo"."HAN_INSPECTION_FACT"  Table__20 ON (Table__20."HAN_INS_ADD_DIM_KEY"=Table__14."HAN_ADD_DIM_KEY")
   INNER JOIN "dbo"."HAN_CASE_DIM"  Table__16 ON (Table__20."HAN_INS_CAS_DIM_KEY"=Table__16."HAN_CAS_DIM_KEY")
   LEFT OUTER JOIN "dbo"."HAN_CASE_LKP"  Table__38 ON (Table__16."HAN_CAS_KEY"=Table__38."LKP_CAS_KEY")
   INNER JOIN "dbo"."HAN_RESOURCE_DIM"  Table__23 ON (Table__20."HAN_INS_PRI_APL_DIM_KEY"=Table__23."HAN_RES_DIM_KEY")
   LEFT OUTER JOIN "dbo"."HAN_INSPECTION_LKP"  Table__37 ON (Table__20."HAN_INS_FACT_KEY"=Table__37."LKP_INS_APCASEINSPKEY")  
WHERE
  (
   Table__16."HAN_CAS_TYPE_DESC"  IN  ( 'Building & Structures Division','Housing Violation'  )
   AND
   Table__23."HAN_RES_DIM_KEY"  NOT IN  ( 1,2  )
  )
GROUP BY
  Table__16."HAN_CAS_AP_NO", 
  convert(date, Table__20."HAN_INS_RESULT_DTTM"), 
  Table__16."HAN_CAS_STATUS", 
  case when Table__38."LKP_CAS_DESCRIPT" is null then Table__37."LKP_INS_DESCRIPT" else Table__38."LKP_CAS_DESCRIPT" end, 
  Table__14."HAN_ADD_BLOCK"
) fullquery
where description is not null
and event_date is not null
and event_date >= DATEADD(month, -60, convert(date, getdate()) )
order by event_date;

	--DDL
CREATE TABLE boston.rs_isd_source
(

	isd_case_id character varying(25) NOT NULL,
	isd_case_dt date,
	isd_status character varying(10),
	isd_description character varying(300),
	isd_sam_id character varying(50),
	isd_etl_load_date date
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_isd_source
  OWNER TO u9as36ung5msfk;

--Initialization
  # SET TODAYS DATE
$G_SYSDATE = cast(sysdate(),'date');
print('Today\'s date: '|| cast($G_SYSDATE, 'varchar(10)'));

----------------------------------------------------------------
--PSQL Data Tables
--SAM LKP
CREATE TABLE boston.rs_sam_lkp
(
  rs_sam_id character varying(50) NOT NULL, 
  rs_sam_full_address character varying(255),
  rs_sam_zip_code character varying(5),
  CONSTRAINT "rs_sam_lkp_pk" PRIMARY KEY (rs_sam_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_sam_lkp
  OWNER TO u9as36ung5msfk;

--MHL HOUSING STAGE 
CREATE TABLE boston.rs_mhl_housing_stg
(
  date date,
  description character varying(50),
  sam_id character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_mhl_housing_stg
  OWNER TO u9as36ung5msfk;
--MHL SANITATION STAGE 
CREATE TABLE boston.rs_mhl_sanitation_stg
(
  date date,
  description character varying(50),
  sam_id character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_mhl_sanitation_stg
  OWNER TO u9as36ung5msfk;
--MHL CIVIC STAGE 
CREATE TABLE boston.rs_mhl_civic_stg
(
  date date,
  description character varying(50),
  sam_id character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_mhl_civic_stg
  OWNER TO u9as36ung5msfk;
--ISD HOUSING STAGE 
CREATE TABLE boston.rs_isd_housing_stg
(
  date date,
  description character varying(300),
  sam_id character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_isd_housing_stg
  OWNER TO u9as36ung5msfk;
--ISD BUILDING STAGE 
CREATE TABLE boston.rs_isd_building_stg
(
  date date,
  description character varying(300),
  sam_id character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_isd_building_stg
  OWNER TO u9as36ung5msfk;
--ISD CODE STAGE 
CREATE TABLE boston.rs_isd_code_stg
(
  date date,
  description character varying(300),
  sam_id character varying(50)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_isd_code_stg
  OWNER TO u9as36ung5msfk;
--RS PROPERTY STAGE
CREATE TABLE boston.rs_property_stg
(
  rs_prop_sam_id character varying(50) NOT NULL,
  rs_prop_address_type character varying(100),
  rs_prop_full_address character varying(255),
  rs_prop_street_number character varying(21),
  rs_prop_full_street_name character varying(255),
  rs_prop_unit_number character varying(15),
  rs_prop_zip_code character varying(5),
  rs_prop_mailing_neighborhood character varying(50),
  rs_prop_long decimal (19, 9),
  rs_prop_lat decimal (19, 9),
  rs_prop_pid character varying(100),
  rs_prop_lu character varying(50),
  rs_prop_owner character varying(50),
  rs_prop_yr_built character varying(50),
  rs_prop_yr_remod int,
  rs_prop_living_area bigint,
  rs_prop_bdrms character varying(50),
  rs_prop_full_bth character varying(50),
  rs_prop_half_bth character varying(50),
  CONSTRAINT "rs_property_stg_pk" PRIMARY KEY (rs_prop_sam_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_property_stg
  OWNER TO u9as36ung5msfk;
--RS PROPERTY MASTER TABLE
CREATE TABLE boston.rs_property_main
(
  rs_prop_sam_id character varying(50) NOT NULL,
  rs_prop_address_type character varying(100),
  rs_prop_full_address character varying(255),
  rs_prop_street_number character varying(21),
  rs_prop_full_street_name character varying(255),
  rs_prop_unit_number character varying(15),
  rs_prop_zip_code character varying(5),
  rs_prop_mailing_neighborhood character varying(50),
  rs_prop_long decimal (19, 9),
  rs_prop_lat decimal (19, 9),
  rs_prop_pid character varying(100),
  rs_prop_lu character varying(50),
  rs_prop_owner character varying(50),
  rs_prop_yr_built character varying(50),
  rs_prop_yr_remod int,
  rs_prop_living_area bigint,
  rs_prop_bdrms character varying(50),
  rs_prop_full_bth character varying(50),
  rs_prop_half_bth character varying(50),
  json_isd_housing_1year json,
  json_isd_housing_3year json,
  json_isd_housing_5year json,
  json_isd_building_1year json,
  json_isd_building_3year json,
  json_isd_building_5year json,
  json_isd_code_1year json,
  json_isd_code_3year json,
  json_isd_code_5year json,
  json_mhl_civic_1year json,
  json_mhl_civic_3year json,
  json_mhl_civic_5year json,
  json_mhl_housing_1year json,
  json_mhl_housing_3year json,
  json_mhl_housing_5year json,
  json_mhl_sanitation_1year json,
  json_mhl_sanitation_3year json,
  json_mhl_sanitation_5year json,
  CONSTRAINT "rs_property_main_pk" PRIMARY KEY (rs_prop_sam_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_property_main
  OWNER TO u9as36ung5msfk;
--Owner Main
CREATE TABLE boston.rs_owner_main
(
  rs_prop_owner character varying(50),
  json_all_owner_props json
)
WITH (
  OIDS=FALSE
);
ALTER TABLE boston.rs_owner_main
  OWNER TO u9as36ung5msfk;