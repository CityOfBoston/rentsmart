<?php
set_time_limit(1500);
ini_set('memory_limit', '1024M');
//Flag the system as being in the maintenance window
putenv("System_Maintenance=ON");

//getting environment variables for database connection
$pg_database = getenv('PG_DATABASE_PROD');
$pg_user = getenv('PG_USER_PROD');
$pg_password = getenv('PG_PASSWORD_PROD');
$pg_host = getenv('PG_HOST_PROD');
$pg_port = getenv('PG_PORT_PROD');
$pg_schema = getenv('PG_SCHEMA_PROD');

//set the connection string
$conn_string = "host={$pg_host} port={$pg_port} dbname={$pg_database} user={$pg_user} password={$pg_password}";

//connect to the database
$dbconn = pg_connect($conn_string) or die("Could not connect");
echo "Successfully connected to database";
echo "<hr>";

$setMaintenanceOnQuery = "UPDATE {$pg_schema}.System_Maintenance SET System_In_Maintenance_Flag = 1;";
$setMaintenanceOnQuery_result = pg_query($dbconn, $setMaintenanceOnQuery);
  if (!$setMaintenanceOnQuery_result) {
    echo "An error occurred while running the sql load query.\n";
    echo pg_last_error($dbconn);
    exit;
  }


$sql_load_query = [];

$sql_load_query[1] = "TRUNCATE TABLE boston.\"rs_sam_lkp\";

INSERT INTO boston.\"rs_sam_lkp\"
SELECT \"rs_sam_source\".\"cob_sam_id\" as \"rs_sam_id\",
    \"rs_sam_source\".\"cob_sam_full_address\" as \"rs_sam_full_address\",
    \"rs_sam_source\".\"cob_sam_zip_code\" as \"rs_sam_zip_code\"
   FROM boston.\"rs_sam_source\"
  ;";

$sql_load_query[2] = "TRUNCATE TABLE boston.\"rs_isd_housing_stg\";

INSERT INTO boston.\"rs_isd_housing_stg\"
SELECT \"rs_isd_source\".\"isd_case_dt\" as \"date\",
    \"rs_isd_source\".\"isd_description\" as \"description\",
    \"rs_isd_source\".\"isd_sam_id\" as \"sam_id\"
   FROM boston.\"rs_isd_source\"
  WHERE \"rs_isd_source\".\"isd_case_id\" LIKE 'H%'::text
  ;";

$sql_load_query[3] = "TRUNCATE TABLE boston.\"rs_isd_building_stg\";

INSERT INTO boston.\"rs_isd_building_stg\"
SELECT \"rs_isd_source\".\"isd_case_dt\" as \"date\",
    \"rs_isd_source\".\"isd_description\" as \"description\",
    \"rs_isd_source\".\"isd_sam_id\" as \"sam_id\"
   FROM boston.\"rs_isd_source\"
  WHERE \"rs_isd_source\".\"isd_case_id\" LIKE 'V%'::text
  ;";

$sql_load_query[4] = "TRUNCATE TABLE boston.\"rs_isd_code_stg\";

INSERT INTO boston.\"rs_isd_code_stg\"
SELECT \"rs_isd_source\".\"isd_case_dt\" as \"date\",
    \"rs_isd_source\".\"isd_description\" as \"description\",
    \"rs_isd_source\".\"isd_sam_id\" as \"sam_id\"
   FROM boston.\"rs_isd_source\"
  WHERE \"rs_isd_source\".\"isd_case_id\" LIKE 'C%'::text
  ;";

$sql_load_query[5] = "TRUNCATE TABLE boston.\"rs_mhl_sanitation_stg\";

INSERT INTO boston.\"rs_mhl_sanitation_stg\"
 SELECT \"rs_mhl_source\".\"mhl_open_dt\" as \"date\",
    \"rs_mhl_source\".\"mhl_type\" as \"description\",
    \"rs_mhl_source\".\"mhl_sam_id\" as \"sam_id\"
   FROM boston.\"rs_mhl_source\"
  WHERE 
  \"rs_mhl_source\".\"mhl_type\" NOT LIKE '%Snow%'
  AND (
    (\"rs_mhl_source\".\"mhl_reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) 
    OR \"rs_mhl_source\".\"mhl_reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"rs_mhl_source\".\"mhl_type\" = 'Abandoned Vehicles'::text 
    OR \"rs_mhl_source\".\"mhl_reason\" = 'Environmental Services'::text AND \"rs_mhl_source\".\"mhl_type\" <> 'Illegal Auto Body Shop'::text 
    OR \"rs_mhl_source\".\"mhl_reason\" = 'Highway Maintenance'::text AND (\"rs_mhl_source\".\"mhl_type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])))
;";

$sql_load_query[6] = "TRUNCATE TABLE boston.\"rs_mhl_housing_stg\";

INSERT INTO boston.\"rs_mhl_housing_stg\"
 SELECT \"rs_mhl_source\".\"mhl_open_dt\" as \"date\",
    \"rs_mhl_source\".\"mhl_type\" as \"description\",
    \"rs_mhl_source\".\"mhl_sam_id\" as \"sam_id\"
   FROM boston.\"rs_mhl_source\"
  WHERE 
  (\"rs_mhl_source\".\"mhl_reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]))
  ;";

$sql_load_query[7] = "TRUNCATE TABLE boston.\"rs_mhl_civic_stg\";

INSERT INTO boston.\"rs_mhl_civic_stg\"
 SELECT \"rs_mhl_source\".\"mhl_open_dt\" as \"date\",
    \"rs_mhl_source\".\"mhl_type\" as \"description\",
    \"rs_mhl_source\".\"mhl_sam_id\" as \"sam_id\"
   FROM boston.\"rs_mhl_source\"
  WHERE 
  ((\"rs_mhl_source\".\"mhl_reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) 
    OR \"rs_mhl_source\".\"mhl_reason\" = 'Highway Maintenance'::text 
      AND (\"rs_mhl_source\".\"mhl_type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])));";

$sql_load_query[8] = "TRUNCATE TABLE boston.\"rs_property_stg\";

INSERT INTO boston.\"rs_property_stg\"
SELECT 
\"sam\".\"cob_sam_id\" as \"rs_prop_sam_id\",
\"sam\".\"cob_sam_prim_sam_id\" as \"rs_prop_prim_sam_id\",
\"sam\".\"cob_sam_full_address\" as \"rs_prop_full_address\",
\"sam\".\"cob_sam_street_number\" as \"rs_prop_street_number\",
\"sam\".\"cob_sam_full_street_name\" as \"rs_prop_full_street_name\",
\"sam\".\"cob_sam_unit_number\" as \"rs_prop_unit_number\",
\"sam\".\"cob_sam_zip_code\" as \"rs_prop_zip_code\",
\"sam\".\"cob_sam_mailing_neighborhood\" as \"rs_prop_mailing_neighborhood\",
ST_X(ST_Transform(ST_SetSRID(ST_Point(\"sam\".\"cob_sam_lat\",\"sam\".\"cob_sam_long\"),2249),4326)) as \"rs_prop_long\",
ST_Y(ST_Transform(ST_SetSRID(ST_Point(\"sam\".\"cob_sam_lat\",\"sam\".\"cob_sam_long\"),2249),4326)) as \"rs_prop_lat\",
\"sam\".\"cob_sam_pid\" as \"rs_prop_pi\",
\"ast\".\"ast_lu\" as \"rs_prop_lu\",
\"ast\".\"ast_owner\" as \"rs_prop_owner\",
\"ast\".\"ast_yr_built\" as \"rs_prop_yr_built\",
\"ast\".\"ast_yr_remod\" as \"rs_prop_yr_remod\",
\"ast\".\"ast_living_area\" as \"rs_prop_living_area\",
COALESCE(\"ast\".\"ast_r_bdrms\"::text, \"ast\".\"ast_u_bdrms\") as \"rs_prop_bdrm\",
COALESCE(\"ast\".\"ast_r_full_bth\"::text, \"ast\".\"ast_u_full_bth\") as \"rs_prop_full_bth\",
COALESCE(\"ast\".\"ast_r_half_bth\"::text, \"ast\".\"ast_u_half_bth\") as \"rs_prop_half_bth\"
FROM boston.\"rs_sam_source\" sam
LEFT OUTER JOIN boston.\"rs_ast_source\" ast
ON \"sam\".\"cob_sam_pid\" = \"ast\".\"ast_pid\";";

$sql_load_query[9] = "TRUNCATE TABLE boston.\"rs_property_main_bldg\";

INSERT INTO boston.\"rs_property_main_bldg\"
SELECT 
\"prop\".\"rs_prop_sam_id\", 
\"prop\".\"rs_prop_prim_sam_id\", 
\"prop\".\"rs_prop_full_address\", 
\"prop\".\"rs_prop_street_number\", 
\"prop\".\"rs_prop_full_street_name\", 
\"prop\".\"rs_prop_unit_number\", 
\"prop\".\"rs_prop_zip_code\", 
\"prop\".\"rs_prop_mailing_neighborhood\", 
\"prop\".\"rs_prop_long\", 
\"prop\".\"rs_prop_lat\", 
\"prop\".\"rs_prop_pid\", 
\"prop\".\"rs_prop_lu\", 
\"prop\".\"rs_prop_owner\", 
\"prop\".\"rs_prop_yr_built\", 
\"prop\".\"rs_prop_yr_remod\", 
\"prop\".\"rs_prop_living_area\", 
\"prop\".\"rs_prop_bdrms\", 
\"prop\".\"rs_prop_full_bth\", 
\"prop\".\"rs_prop_half_bth\", 
\"isdh1\".\"json_isd_housing_1year\", 
\"isdh3\".\"json_isd_housing_3year\", 
\"isdh5\".\"json_isd_housing_5year\", 
\"isdb1\".\"json_isd_building_1year\", 
\"isdb3\".\"json_isd_building_3year\", 
\"isdb5\".\"json_isd_building_5year\", 
\"isdc1\".\"json_isd_code_1year\", 
\"isdc3\".\"json_isd_code_3year\", 
\"isdc5\".\"json_isd_code_5year\", 
\"mhlc1\".\"json_mhl_civic_1year\", 
\"mhlc3\".\"json_mhl_civic_3year\", 
\"mhlc5\".\"json_mhl_civic_5year\", 
\"mhlh1\".\"json_mhl_housing_1year\", 
\"mhlh3\".\"json_mhl_housing_3year\", 
\"mhlh5\".\"json_mhl_housing_5year\", 
\"mhls1\".\"json_mhl_sanitation_1year\", 
\"mhls3\".\"json_mhl_sanitation_3year\", 
\"mhls5\".\"json_mhl_sanitation_5year\",


CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbh1\".\"json_bldg_isd_housing_1year\" END json_bldg_isd_housing_1year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbh3\".\"json_bldg_isd_housing_3year\" END json_bldg_isd_housing_3year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbh5\".\"json_bldg_isd_housing_5year\" END json_bldg_isd_housing_5year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbb1\".\"json_bldg_isd_building_1year\" END json_bldg_isd_building_1year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbb3\".\"json_bldg_isd_building_3year\" END json_bldg_isd_building_3year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbb5\".\"json_bldg_isd_building_5year\" END json_bldg_isd_building_5year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbc1\".\"json_bldg_isd_code_1year\" END json_bldg_isd_code_1year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbc3\".\"json_bldg_isd_code_3year\" END json_bldg_isd_code_3year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"isdbc5\".\"json_bldg_isd_code_5year\" END json_bldg_isd_code_5year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbc1\".\"json_bldg_mhl_civic_1year\" END json_bldg_mhl_civic_1year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbc3\".\"json_bldg_mhl_civic_3year\" END json_bldg_mhl_civic_3year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbc5\".\"json_bldg_mhl_civic_5year\" END json_bldg_mhl_civic_5year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbh1\".\"json_bldg_mhl_housing_1year\" END json_bldg_mhl_housing_1year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbh3\".\"json_bldg_mhl_housing_3year\" END json_bldg_mhl_housing_3year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbh5\".\"json_bldg_mhl_housing_5year\" END json_bldg_mhl_housing_5year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbs1\".\"json_bldg_mhl_sanitation_1year\" END json_bldg_mhl_sanitation_1year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbs3\".\"json_bldg_mhl_sanitation_3year\" END json_bldg_mhl_sanitation_3year,
CASE WHEN \"prop\".\"rs_prop_sam_id\" = \"prop\".\"rs_prop_prim_sam_id\" THEN NULL ELSE \"mhlbs5\".\"json_bldg_mhl_sanitation_5year\" END json_bldg_mhl_sanitation_5year
FROM boston.\"rs_property_stg\" prop
--ISD Housing 1 year
LEFT JOIN (SELECT   \"rs_isd_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_housing_stg\".* ORDER BY date DESC)) as \"json_isd_housing_1year\"
      FROM boston.\"rs_isd_housing_stg\"
      WHERE \"rs_isd_housing_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_isd_housing_stg\".\"sam_id\") isdh1
  ON \"prop\".\"rs_prop_sam_id\" = \"isdh1\".\"sam_id\"
--ISD Housing 3 year
LEFT JOIN (SELECT   \"rs_isd_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_housing_stg\".* ORDER BY date DESC)) as \"json_isd_housing_3year\"
      FROM boston.\"rs_isd_housing_stg\"
      WHERE \"rs_isd_housing_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_isd_housing_stg\".\"sam_id\") isdh3
  ON \"prop\".\"rs_prop_sam_id\" = \"isdh3\".\"sam_id\"
--ISD Housing 5 year
LEFT JOIN (SELECT   \"rs_isd_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_housing_stg\".* ORDER BY date DESC)) as \"json_isd_housing_5year\"
      FROM boston.\"rs_isd_housing_stg\"
      WHERE \"rs_isd_housing_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_isd_housing_stg\".\"sam_id\") isdh5
  ON \"prop\".\"rs_prop_sam_id\" = \"isdh5\".\"sam_id\"
--ISD Building 1 year
LEFT JOIN (SELECT   \"rs_isd_building_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_building_stg\".* ORDER BY date DESC)) as \"json_isd_building_1year\"
      FROM boston.\"rs_isd_building_stg\"
      WHERE \"rs_isd_building_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_isd_building_stg\".\"sam_id\") isdb1
  ON \"prop\".\"rs_prop_sam_id\" = \"isdb1\".\"sam_id\"
--ISD Building 3 year
LEFT JOIN (SELECT   \"rs_isd_building_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_building_stg\".* ORDER BY date DESC)) as \"json_isd_building_3year\"
      FROM boston.\"rs_isd_building_stg\"
      WHERE \"rs_isd_building_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_isd_building_stg\".\"sam_id\") isdb3
  ON \"prop\".\"rs_prop_sam_id\" = \"isdb3\".\"sam_id\"
--ISD Building 5 year
LEFT JOIN (SELECT   \"rs_isd_building_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_building_stg\".* ORDER BY date DESC)) as \"json_isd_building_5year\"
      FROM boston.\"rs_isd_building_stg\"
      WHERE \"rs_isd_building_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_isd_building_stg\".\"sam_id\") isdb5
  ON \"prop\".\"rs_prop_sam_id\" = \"isdb5\".\"sam_id\"
--ISD Code 1 year
LEFT JOIN (SELECT   \"rs_isd_code_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_code_stg\".* ORDER BY date DESC)) as \"json_isd_code_1year\"
      FROM boston.\"rs_isd_code_stg\"
      WHERE \"rs_isd_code_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_isd_code_stg\".\"sam_id\") isdc1
  ON \"prop\".\"rs_prop_sam_id\" = \"isdc1\".\"sam_id\"
--ISD Code 3 year
LEFT JOIN (SELECT   \"rs_isd_code_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_code_stg\".* ORDER BY date DESC)) as \"json_isd_code_3year\"
      FROM boston.\"rs_isd_code_stg\"
      WHERE \"rs_isd_code_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_isd_code_stg\".\"sam_id\") isdc3
  ON \"prop\".\"rs_prop_sam_id\" = \"isdc3\".\"sam_id\"
--ISD Code 5 year
LEFT JOIN (SELECT   \"rs_isd_code_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_code_stg\".* ORDER BY date DESC)) as \"json_isd_code_5year\"
      FROM boston.\"rs_isd_code_stg\"
      WHERE \"rs_isd_code_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_isd_code_stg\".\"sam_id\") isdc5
  ON \"prop\".\"rs_prop_sam_id\" = \"isdc5\".\"sam_id\"
--MHL Civic 1 year
LEFT JOIN (SELECT   \"rs_mhl_civic_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_civic_stg\".* ORDER BY date DESC)) as \"json_mhl_civic_1year\"
      FROM boston.\"rs_mhl_civic_stg\"
      WHERE \"rs_mhl_civic_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_mhl_civic_stg\".\"sam_id\") mhlc1
  ON \"prop\".\"rs_prop_sam_id\" = \"mhlc1\".\"sam_id\"
--MHL Civic 3 year
LEFT JOIN (SELECT   \"rs_mhl_civic_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_civic_stg\".* ORDER BY date DESC)) as \"json_mhl_civic_3year\"
      FROM boston.\"rs_mhl_civic_stg\"
      WHERE \"rs_mhl_civic_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_mhl_civic_stg\".\"sam_id\") mhlc3
  ON \"prop\".\"rs_prop_sam_id\" = \"mhlc3\".\"sam_id\"
--MHL Civic 5 year
LEFT JOIN (SELECT   \"rs_mhl_civic_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_civic_stg\".* ORDER BY date DESC)) as \"json_mhl_civic_5year\"
      FROM boston.\"rs_mhl_civic_stg\"
      WHERE \"rs_mhl_civic_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_mhl_civic_stg\".\"sam_id\") mhlc5
  ON \"prop\".\"rs_prop_sam_id\" = \"mhlc5\".\"sam_id\"
--MHL housing 1 year
LEFT JOIN (SELECT   \"rs_mhl_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_housing_stg\".* ORDER BY date DESC)) as \"json_mhl_housing_1year\"
      FROM boston.\"rs_mhl_housing_stg\"
      WHERE \"rs_mhl_housing_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_mhl_housing_stg\".\"sam_id\") mhlh1
  ON \"prop\".\"rs_prop_sam_id\" = \"mhlh1\".\"sam_id\"
--MHL housing 3 year
LEFT JOIN (SELECT   \"rs_mhl_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_housing_stg\".* ORDER BY date DESC)) as \"json_mhl_housing_3year\"
      FROM boston.\"rs_mhl_housing_stg\"
      WHERE \"rs_mhl_housing_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_mhl_housing_stg\".\"sam_id\") mhlh3
  ON \"prop\".\"rs_prop_sam_id\" = \"mhlh3\".\"sam_id\"
--MHL housing 5 year
LEFT JOIN (SELECT   \"rs_mhl_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_housing_stg\".* ORDER BY date DESC)) as \"json_mhl_housing_5year\"
      FROM boston.\"rs_mhl_housing_stg\"
      WHERE \"rs_mhl_housing_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_mhl_housing_stg\".\"sam_id\") mhlh5
  ON \"prop\".\"rs_prop_sam_id\" = \"mhlh5\".\"sam_id\"
--MHL sanitation 1 year
LEFT JOIN (SELECT   \"rs_mhl_sanitation_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_sanitation_stg\".* ORDER BY date DESC)) as \"json_mhl_sanitation_1year\"
      FROM boston.\"rs_mhl_sanitation_stg\" 
      WHERE \"rs_mhl_sanitation_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_mhl_sanitation_stg\".\"sam_id\") mhls1
  ON \"prop\".\"rs_prop_sam_id\" = \"mhls1\".\"sam_id\"
--MHL sanitation 3 year
LEFT JOIN (SELECT   \"rs_mhl_sanitation_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_sanitation_stg\".* ORDER BY date DESC)) as \"json_mhl_sanitation_3year\"
      FROM boston.\"rs_mhl_sanitation_stg\" 
      WHERE \"rs_mhl_sanitation_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_mhl_sanitation_stg\".\"sam_id\") mhls3
  ON \"prop\".\"rs_prop_sam_id\" = \"mhls3\".\"sam_id\"
--MHL sanitation 5 year
LEFT JOIN (SELECT   \"rs_mhl_sanitation_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_sanitation_stg\".* ORDER BY date DESC)) as \"json_mhl_sanitation_5year\"
      FROM boston.\"rs_mhl_sanitation_stg\" 
      WHERE \"rs_mhl_sanitation_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_mhl_sanitation_stg\".\"sam_id\") mhls5
  ON \"prop\".\"rs_prop_sam_id\" = \"mhls5\".\"sam_id\"
  --ISD Housing 1 year
LEFT JOIN (SELECT   \"rs_isd_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_housing_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_housing_1year\"
      FROM boston.\"rs_isd_housing_stg\"
      WHERE \"rs_isd_housing_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_isd_housing_stg\".\"sam_id\") isdbh1
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbh1\".\"sam_id\"
--ISD Housing 3 year
LEFT JOIN (SELECT   \"rs_isd_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_housing_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_housing_3year\"
      FROM boston.\"rs_isd_housing_stg\"
      WHERE \"rs_isd_housing_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_isd_housing_stg\".\"sam_id\") isdbh3
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbh3\".\"sam_id\"
--ISD Housing 5 year
LEFT JOIN (SELECT   \"rs_isd_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_housing_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_housing_5year\"
      FROM boston.\"rs_isd_housing_stg\"
      WHERE \"rs_isd_housing_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_isd_housing_stg\".\"sam_id\") isdbh5
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbh5\".\"sam_id\"
--ISD Building 1 year
LEFT JOIN (SELECT   \"rs_isd_building_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_building_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_building_1year\"
      FROM boston.\"rs_isd_building_stg\"
      WHERE \"rs_isd_building_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_isd_building_stg\".\"sam_id\") isdbb1
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbb1\".\"sam_id\"
--ISD Building 3 year
LEFT JOIN (SELECT   \"rs_isd_building_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_building_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_building_3year\"
      FROM boston.\"rs_isd_building_stg\"
      WHERE \"rs_isd_building_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_isd_building_stg\".\"sam_id\") isdbb3
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbb3\".\"sam_id\"
--ISD Building 5 year
LEFT JOIN (SELECT   \"rs_isd_building_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_building_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_building_5year\"
      FROM boston.\"rs_isd_building_stg\"
      WHERE \"rs_isd_building_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_isd_building_stg\".\"sam_id\") isdbb5
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbb5\".\"sam_id\"
--ISD Code 1 year
LEFT JOIN (SELECT   \"rs_isd_code_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_code_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_code_1year\"
      FROM boston.\"rs_isd_code_stg\"
      WHERE \"rs_isd_code_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_isd_code_stg\".\"sam_id\") isdbc1
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbc1\".\"sam_id\"
--ISD Code 3 year
LEFT JOIN (SELECT   \"rs_isd_code_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_code_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_code_3year\"
      FROM boston.\"rs_isd_code_stg\"
      WHERE \"rs_isd_code_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_isd_code_stg\".\"sam_id\") isdbc3
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbc3\".\"sam_id\"
--ISD Code 5 year
LEFT JOIN (SELECT   \"rs_isd_code_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_isd_code_stg\".* ORDER BY date DESC)) as \"json_bldg_isd_code_5year\"
      FROM boston.\"rs_isd_code_stg\"
      WHERE \"rs_isd_code_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_isd_code_stg\".\"sam_id\") isdbc5
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"isdbc5\".\"sam_id\"
--MHL Civic 1 year
LEFT JOIN (SELECT   \"rs_mhl_civic_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_civic_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_civic_1year\"
      FROM boston.\"rs_mhl_civic_stg\"
      WHERE \"rs_mhl_civic_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_mhl_civic_stg\".\"sam_id\") mhlbc1
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbc1\".\"sam_id\"
--MHL Civic 3 year
LEFT JOIN (SELECT   \"rs_mhl_civic_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_civic_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_civic_3year\"
      FROM boston.\"rs_mhl_civic_stg\"
      WHERE \"rs_mhl_civic_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_mhl_civic_stg\".\"sam_id\") mhlbc3
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbc3\".\"sam_id\"
--MHL Civic 5 year
LEFT JOIN (SELECT   \"rs_mhl_civic_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_civic_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_civic_5year\"
      FROM boston.\"rs_mhl_civic_stg\"
      WHERE \"rs_mhl_civic_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_mhl_civic_stg\".\"sam_id\") mhlbc5
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbc5\".\"sam_id\"
--MHL housing 1 year
LEFT JOIN (SELECT   \"rs_mhl_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_housing_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_housing_1year\"
      FROM boston.\"rs_mhl_housing_stg\"
      WHERE \"rs_mhl_housing_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_mhl_housing_stg\".\"sam_id\") mhlbh1
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbh1\".\"sam_id\"
--MHL housing 3 year
LEFT JOIN (SELECT   \"rs_mhl_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_housing_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_housing_3year\"
      FROM boston.\"rs_mhl_housing_stg\"
      WHERE \"rs_mhl_housing_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_mhl_housing_stg\".\"sam_id\") mhlbh3
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbh3\".\"sam_id\"
--MHL housing 5 year
LEFT JOIN (SELECT   \"rs_mhl_housing_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_housing_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_housing_5year\"
      FROM boston.\"rs_mhl_housing_stg\"
      WHERE \"rs_mhl_housing_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_mhl_housing_stg\".\"sam_id\") mhlbh5
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbh5\".\"sam_id\"
--MHL sanitation 1 year
LEFT JOIN (SELECT   \"rs_mhl_sanitation_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_sanitation_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_sanitation_1year\"
      FROM boston.\"rs_mhl_sanitation_stg\" 
      WHERE \"rs_mhl_sanitation_stg\".\"date\" >  (now()::date - interval '1 year')
      GROUP BY \"rs_mhl_sanitation_stg\".\"sam_id\") mhlbs1
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbs1\".\"sam_id\"
--MHL sanitation 3 year
LEFT JOIN (SELECT   \"rs_mhl_sanitation_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_sanitation_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_sanitation_3year\"
      FROM boston.\"rs_mhl_sanitation_stg\" 
      WHERE \"rs_mhl_sanitation_stg\".\"date\" >  (now()::date - interval '3 years')
      GROUP BY \"rs_mhl_sanitation_stg\".\"sam_id\") mhlbs3
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbs3\".\"sam_id\"
--MHL sanitation 5 year
LEFT JOIN (SELECT   \"rs_mhl_sanitation_stg\".\"sam_id\",
          array_to_json(array_agg(\"rs_mhl_sanitation_stg\".* ORDER BY date DESC)) as \"json_bldg_mhl_sanitation_5year\"
      FROM boston.\"rs_mhl_sanitation_stg\" 
      WHERE \"rs_mhl_sanitation_stg\".\"date\" >  (now()::date - interval '5 years')
      GROUP BY \"rs_mhl_sanitation_stg\".\"sam_id\") mhlbs5
  ON \"prop\".\"rs_prop_prim_sam_id\" = \"mhlbs5\".\"sam_id\";";

$sql_load_query[10] = "TRUNCATE TABLE boston.\"rs_owner_main\";

INSERT INTO boston.\"rs_owner_main\"
SELECT  \"rps\".\"rs_prop_owner\",
    array_to_json(array_agg(\"rps\".* ORDER BY \"rs_prop_full_address\")) as \"json_all_owner_props\"
FROM 
(SELECT \"rs_property_stg\".\"rs_prop_owner\", \"rs_property_stg\".\"rs_prop_full_address\", \"rs_property_stg\".\"rs_prop_zip_code\"
FROM boston.\"rs_property_stg\") rps
GROUP BY \"rps\".\"rs_prop_owner\";";

$sql_load_query[11] = "VACUUM ANALYZE;";

echo "Successfully prepared SQL queries.";
echo "<hr>";
//run the sql load query
echo "Running the database preparation and loading scripts"; 
echo "<hr>";

foreach ($sql_load_query as $key => $value) {
  $sql_load_result = pg_query($dbconn, $value);
  if (!$sql_load_result) {
    echo "An error occurred while running the sql load query.\n";
    echo pg_last_error($dbconn);
    exit;
  }
}
//Take the system out of maintenance mode
$setMaintenanceOffQuery = "UPDATE {$pg_schema}.System_Maintenance SET System_In_Maintenance_Flag = 0;";
$setMaintenanceOffQuery_result = pg_query($dbconn, $setMaintenanceOffQuery);
  if (!$setMaintenanceOffQuery_result) {
    echo "An error occurred while running the sql load query.\n";
    echo pg_last_error($dbconn);
    exit;
  }

// Closing connection
pg_close($dbconn);
echo("Closing the connection");
exit;
?>

