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

$sql_load_query[1] = "TRUNCATE TABLE boston.\"temp_housing_violation\";

INSERT INTO boston.\"temp_housing_violation\"
SELECT \"STG_CODE_VIOLATIONS\".\"Latitude\",
    \"STG_CODE_VIOLATIONS\".\"Longitude\",
    \"STG_CODE_VIOLATIONS\".\"Violation_Description\",
    \"STG_CODE_VIOLATIONS\".\"Date\"
   FROM boston.\"STG_CODE_VIOLATIONS\"
  WHERE \"STG_CODE_VIOLATIONS\".\"Enforcement_Type\" = 'Housing'::text;";
$sql_load_query[2] = "TRUNCATE TABLE boston.\"temp_building_violation\";

INSERT INTO boston.\"temp_building_violation\"
SELECT \"STG_CODE_VIOLATIONS\".\"Latitude\",
    \"STG_CODE_VIOLATIONS\".\"Longitude\",
    \"STG_CODE_VIOLATIONS\".\"Violation_Description\",
    \"STG_CODE_VIOLATIONS\".\"Date\"
   FROM boston.\"STG_CODE_VIOLATIONS\"
  WHERE \"STG_CODE_VIOLATIONS\".\"Enforcement_Type\" = 'Building'::text;";
$sql_load_query[3] = "TRUNCATE TABLE boston.\"temp_code_violation\";

INSERT INTO boston.\"temp_code_violation\"
SELECT \"STG_CODE_VIOLATIONS\".\"Latitude\",
    \"STG_CODE_VIOLATIONS\".\"Longitude\",
    \"STG_CODE_VIOLATIONS\".\"Violation_Description\",
    \"STG_CODE_VIOLATIONS\".\"Date\"
   FROM boston.\"STG_CODE_VIOLATIONS\"
  WHERE \"STG_CODE_VIOLATIONS\".\"Enforcement_Type\" = 'Code'::text;";
$sql_load_query[4] = "TRUNCATE TABLE boston.\"temp_nvcrime_3mth\";

INSERT INTO boston.\"temp_nvcrime_3mth\"
 SELECT \"STG_CRIME\".\"Incident_Description\" AS \"Description\",
    \"STG_CRIME\".\"Latitude\",
    \"STG_CRIME\".\"Longitude\",
    \"STG_CRIME\".\"Date\"
   FROM boston.\"STG_CRIME\"
  WHERE \"STG_CRIME\".\"Date\" >= ('now'::text::date - 90) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part two'::text;";
$sql_load_query[5] = "TRUNCATE TABLE boston.\"temp_nvcrime_6mth\";

INSERT INTO boston.\"temp_nvcrime_6mth\"
 SELECT \"STG_CRIME\".\"Incident_Description\" AS \"Description\",
    \"STG_CRIME\".\"Latitude\",
    \"STG_CRIME\".\"Longitude\",
    \"STG_CRIME\".\"Date\"
   FROM boston.\"STG_CRIME\"
  WHERE \"STG_CRIME\".\"Date\" >= ('now'::text::date - 180) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part two'::text;";
$sql_load_query[6] = "TRUNCATE TABLE boston.\"temp_nvcrime_12mth\";

INSERT INTO boston.\"temp_nvcrime_12mth\"
 SELECT \"STG_CRIME\".\"Incident_Description\" AS \"Description\",
    \"STG_CRIME\".\"Latitude\",
    \"STG_CRIME\".\"Longitude\",
    \"STG_CRIME\".\"Date\"
   FROM boston.\"STG_CRIME\"
  WHERE \"STG_CRIME\".\"Date\" >= ('now'::text::date - 365) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part two'::text;";
$sql_load_query[7] = "TRUNCATE TABLE boston.\"temp_vcrime_3mth\";

INSERT INTO boston.\"temp_vcrime_3mth\"
 SELECT \"STG_CRIME\".\"Incident_Description\" AS \"Description\",
    \"STG_CRIME\".\"Latitude\",
    \"STG_CRIME\".\"Longitude\",
    \"STG_CRIME\".\"Date\"
   FROM boston.\"STG_CRIME\"
  WHERE \"STG_CRIME\".\"Date\" >= ('now'::text::date - 90) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part one'::text;";
$sql_load_query[8] = "TRUNCATE TABLE boston.\"temp_vcrime_6mth\";

INSERT INTO boston.\"temp_vcrime_6mth\"
 SELECT \"STG_CRIME\".\"Incident_Description\" AS \"Description\",
    \"STG_CRIME\".\"Latitude\",
    \"STG_CRIME\".\"Longitude\",
    \"STG_CRIME\".\"Date\"
   FROM boston.\"STG_CRIME\"
  WHERE \"STG_CRIME\".\"Date\" >= ('now'::text::date - 180) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part one'::text;";
$sql_load_query[9] = "TRUNCATE TABLE boston.\"temp_vcrime_12mth\";

INSERT INTO boston.\"temp_vcrime_12mth\"
 SELECT \"STG_CRIME\".\"Incident_Description\" AS \"Description\",
    \"STG_CRIME\".\"Latitude\",
    \"STG_CRIME\".\"Longitude\",
    \"STG_CRIME\".\"Date\"
   FROM boston.\"STG_CRIME\"
  WHERE \"STG_CRIME\".\"Date\" >= ('now'::text::date - 365) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part one'::text;";
$sql_load_query[10] = "TRUNCATE TABLE boston.\"temp_noise_3mth\";
 
INSERT INTO boston.\"temp_noise_3mth\"
 SELECT 'Noise Violation'::text AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Generic Noise Disturbance'::text, 'Noise Disturbance'::text, 'Massport'::text, 'Neighborhood Services Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Animal Issues'::text AND \"STG_HOTLINE\".\"Type\" = 'Animal Noise Disturbances'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND \"STG_HOTLINE\".\"Type\" = 'Work Hours-Loud Noise Complaints'::text);";
$sql_load_query[11] = "TRUNCATE TABLE boston.\"temp_noise_6mth\";

INSERT INTO boston.\"temp_noise_6mth\"
 SELECT 'Noise Violation'::text AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Generic Noise Disturbance'::text, 'Noise Disturbance'::text, 'Massport'::text, 'Neighborhood Services Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Animal Issues'::text AND \"STG_HOTLINE\".\"Type\" = 'Animal Noise Disturbances'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND \"STG_HOTLINE\".\"Type\" = 'Work Hours-Loud Noise Complaints'::text);";
$sql_load_query[12] = "TRUNCATE TABLE boston.\"temp_noise_12mth\";

INSERT INTO boston.\"temp_noise_12mth\"
 SELECT 'Noise Violation'::text AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Generic Noise Disturbance'::text, 'Noise Disturbance'::text, 'Massport'::text, 'Neighborhood Services Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Animal Issues'::text AND \"STG_HOTLINE\".\"Type\" = 'Animal Noise Disturbances'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND \"STG_HOTLINE\".\"Type\" = 'Work Hours-Loud Noise Complaints'::text);";
$sql_load_query[13] = "TRUNCATE TABLE boston.\"temp_sanitation_3mth\";

INSERT INTO boston.\"temp_sanitation_3mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND \"STG_HOTLINE\".\"Type\" NOT LIKE '%Snow%'AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"STG_HOTLINE\".\"Type\" = 'Abandoned Vehicles'::text OR \"STG_HOTLINE\".\"Reason\" = 'Environmental Services'::text AND \"STG_HOTLINE\".\"Type\" <> 'Illegal Auto Body Shop'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])));";
$sql_load_query[14] = "TRUNCATE TABLE boston.\"temp_sanitation_6mth\";

INSERT INTO boston.\"temp_sanitation_6mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND \"STG_HOTLINE\".\"Type\" NOT LIKE '%Snow%'AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"STG_HOTLINE\".\"Type\" = 'Abandoned Vehicles'::text OR \"STG_HOTLINE\".\"Reason\" = 'Environmental Services'::text AND \"STG_HOTLINE\".\"Type\" <> 'Illegal Auto Body Shop'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])));";
$sql_load_query[15] = "TRUNCATE TABLE boston.\"temp_sanitation_12mth\";

INSERT INTO boston.\"temp_sanitation_12mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND \"STG_HOTLINE\".\"Type\" NOT LIKE '%Snow%'AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"STG_HOTLINE\".\"Type\" = 'Abandoned Vehicles'::text OR \"STG_HOTLINE\".\"Reason\" = 'Environmental Services'::text AND \"STG_HOTLINE\".\"Type\" <> 'Illegal Auto Body Shop'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])));";
$sql_load_query[16] = "TRUNCATE TABLE boston.\"temp_housing_3mth\";

INSERT INTO boston.\"temp_housing_3mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND (\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]));";
$sql_load_query[17] = "TRUNCATE TABLE boston.\"temp_housing_6mth\";

INSERT INTO boston.\"temp_housing_6mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND (\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]));";
$sql_load_query[18] = "TRUNCATE TABLE boston.\"temp_housing_12mth\";

INSERT INTO boston.\"temp_housing_12mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
   FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND (\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]));";
$sql_load_query[19] = "TRUNCATE TABLE boston.\"temp_civic_3mth\";

INSERT INTO boston.\"temp_civic_3mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])));";
$sql_load_query[20] = "TRUNCATE TABLE boston.\"temp_civic_6mth\";

INSERT INTO boston.\"temp_civic_6mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])));";
$sql_load_query[21] = "TRUNCATE TABLE boston.\"temp_civic_12mth\";

INSERT INTO boston.\"temp_civic_12mth\"
 SELECT \"STG_HOTLINE\".\"Type\" AS \"Description\",
    \"STG_HOTLINE\".\"Latitude\",
    \"STG_HOTLINE\".\"Longitude\",
    \"STG_HOTLINE\".\"Open_Date\" AS \"Date\"
FROM boston.\"STG_HOTLINE\"
  WHERE \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])));";

$sql_load_query[22] = "UPDATE boston.\"temp_nvcrime_3mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_nvcrime_3mth\".\"Longitude\", \"temp_nvcrime_3mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_nvcrime_6mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_nvcrime_6mth\".\"Longitude\", \"temp_nvcrime_6mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_nvcrime_12mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_nvcrime_12mth\".\"Longitude\", \"temp_nvcrime_12mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_vcrime_3mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_vcrime_3mth\".\"Longitude\", \"temp_vcrime_3mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_vcrime_6mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_vcrime_6mth\".\"Longitude\", \"temp_vcrime_6mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_vcrime_12mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_vcrime_12mth\".\"Longitude\", \"temp_vcrime_12mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_civic_3mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_civic_3mth\".\"Longitude\", \"temp_civic_3mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_civic_6mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_civic_6mth\".\"Longitude\", \"temp_civic_6mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_civic_12mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_civic_12mth\".\"Longitude\", \"temp_civic_12mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_housing_3mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_housing_3mth\".\"Longitude\", \"temp_housing_3mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_housing_6mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_housing_6mth\".\"Longitude\", \"temp_housing_6mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_housing_12mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_housing_12mth\".\"Longitude\", \"temp_housing_12mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_noise_3mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_noise_3mth\".\"Longitude\", \"temp_noise_3mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_noise_6mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_noise_6mth\".\"Longitude\", \"temp_noise_6mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_noise_12mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_noise_12mth\".\"Longitude\", \"temp_noise_12mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_sanitation_3mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_sanitation_3mth\".\"Longitude\", \"temp_sanitation_3mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_sanitation_6mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_sanitation_6mth\".\"Longitude\", \"temp_sanitation_6mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_sanitation_12mth\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_sanitation_12mth\".\"Longitude\", \"temp_sanitation_12mth\".\"Latitude\"),4326);

UPDATE boston.\"temp_building_violation\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_building_violation\".\"Longitude\", \"temp_building_violation\".\"Latitude\"),4326);

UPDATE boston.\"temp_code_violation\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_code_violation\".\"Longitude\", \"temp_code_violation\".\"Latitude\"),4326);

UPDATE boston.\"temp_housing_violation\"
SET the_geom =
ST_SetSRID(ST_MakePoint(\"temp_housing_violation\".\"Longitude\", \"temp_housing_violation\".\"Latitude\"),4326);";

$sql_load_query[23] = "TRUNCATE TABLE boston.\"MASTER_ZIPCODE_COUNTS\";

INSERT INTO boston.\"MASTER_ZIPCODE_COUNTS\"
SELECT a.\"Zipcode\",
(SELECT
count(*)
  FROM boston.\"STG_CRIME\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_CRIME\".\"Longitude\", \"STG_CRIME\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_CRIME\".\"Date\" >= ('now'::text::date - 90) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part two'::text
  ) AS \"count_nvcrime_3mth\",
(SELECT
count(*)
  FROM boston.\"STG_CRIME\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_CRIME\".\"Longitude\", \"STG_CRIME\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_CRIME\".\"Date\" >= ('now'::text::date - 180) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part two'::text
  ) AS \"count_nvcrime_6mth\",
(SELECT
count(*)
  FROM boston.\"STG_CRIME\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_CRIME\".\"Longitude\", \"STG_CRIME\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_CRIME\".\"Date\" >= ('now'::text::date - 365) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part two'::text
  ) AS \"count_nvcrime_12mth\",
(SELECT
count(*)
  FROM boston.\"STG_CRIME\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_CRIME\".\"Longitude\", \"STG_CRIME\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_CRIME\".\"Date\" >= ('now'::text::date - 90) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part one'::text
  ) AS \"count_vcrime_3mth\",
(SELECT
count(*)
  FROM boston.\"STG_CRIME\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_CRIME\".\"Longitude\", \"STG_CRIME\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_CRIME\".\"Date\" >= ('now'::text::date - 180) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part one'::text
  ) AS \"count_vcrime_6mth\",
(SELECT
count(*)
  FROM boston.\"STG_CRIME\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_CRIME\".\"Longitude\", \"STG_CRIME\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_CRIME\".\"Date\" >= ('now'::text::date - 365) AND lower(\"STG_CRIME\".\"Crime_Part\") = 'part one'::text
  ) AS \"count_vcrime_12mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])))
) AS \"count_hotline_civic_3mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])))
) AS \"count_hotline_civic_6mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Catchbasin'::text, 'Pothole'::text, 'Sidewalk Cover / Manhole'::text, 'Signs & Signals'::text, 'Street Lights'::text, 'Trees'::text, 'Water Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Contractor Complaints'::text, 'Highway Maintenance'::text, 'News Boxes'::text, 'Pothole Repair (Internal)'::text, 'PWD Graffiti'::text, 'Request for Pothole Repair'::text, 'Roadway Repair'::text, 'Sidewalk Repair'::text, 'Sidewalk Repair (Internal)'::text, 'Sidewalk Repair (Make Safe)'::text, 'Utility Call-In'::text, 'Utility Casting Repair'::text])))
) AS \"count_hotline_civic_12mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND (\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]))
) AS \"count_hotline_housing_3mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND (\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]))
) AS \"count_hotline_housing_6mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND (\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Air Pollution Control'::text, 'Housing'::text, 'Building'::text]))
) AS \"count_hotline_housing_12mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"STG_HOTLINE\".\"Type\" = 'Abandoned Vehicles'::text OR \"STG_HOTLINE\".\"Reason\" = 'Environmental Services'::text AND \"STG_HOTLINE\".\"Type\" <> 'Illegal Auto Body Shop'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])))
) AS \"count_hotline_sanitation_3mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"STG_HOTLINE\".\"Type\" = 'Abandoned Vehicles'::text OR \"STG_HOTLINE\".\"Reason\" = 'Environmental Services'::text AND \"STG_HOTLINE\".\"Type\" <> 'Illegal Auto Body Shop'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])))
) AS \"count_hotline_sanitation_6mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Abandoned Bicycle'::text, 'Graffiti'::text, 'Street Cleaning'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Enforcement & Abandoned Vehicles'::text AND \"STG_HOTLINE\".\"Type\" = 'Abandoned Vehicles'::text OR \"STG_HOTLINE\".\"Reason\" = 'Environmental Services'::text AND \"STG_HOTLINE\".\"Type\" <> 'Illegal Auto Body Shop'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND (\"STG_HOTLINE\".\"Type\" = ANY (ARRAY['Litter Basket Maintenance'::text, 'Empty Litter Basket'::text])))
) AS \"count_hotline_sanitation_12mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 90) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Generic Noise Disturbance'::text, 'Noise Disturbance'::text, 'Massport'::text, 'Neighborhood Services Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Animal Issues'::text AND \"STG_HOTLINE\".\"Type\" = 'Animal Noise Disturbances'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND \"STG_HOTLINE\".\"Type\" = 'Work Hours-Loud Noise Complaints'::text)
) AS \"count_hotline_noise_3mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 180) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Generic Noise Disturbance'::text, 'Noise Disturbance'::text, 'Massport'::text, 'Neighborhood Services Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Animal Issues'::text AND \"STG_HOTLINE\".\"Type\" = 'Animal Noise Disturbances'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND \"STG_HOTLINE\".\"Type\" = 'Work Hours-Loud Noise Complaints'::text)
) AS \"count_hotline_noise_6mth\",
(SELECT
count(*)
  FROM boston.\"STG_HOTLINE\", boston.\"REF_ZIPCODES\" b
  WHERE ST_Contains(b.polygon,ST_SetSRID(ST_MakePoint(\"STG_HOTLINE\".\"Longitude\", \"STG_HOTLINE\".\"Latitude\"),4326))
  AND a.\"Zipcode\" = b.\"Zipcode\"
  AND \"STG_HOTLINE\".\"Open_Date\" > ('now'::text::date - 365) AND ((\"STG_HOTLINE\".\"Reason\" = ANY (ARRAY['Generic Noise Disturbance'::text, 'Noise Disturbance'::text, 'Massport'::text, 'Neighborhood Services Issues'::text])) OR \"STG_HOTLINE\".\"Reason\" = 'Animal Issues'::text AND \"STG_HOTLINE\".\"Type\" = 'Animal Noise Disturbances'::text OR \"STG_HOTLINE\".\"Reason\" = 'Highway Maintenance'::text AND \"STG_HOTLINE\".\"Type\" = 'Work Hours-Loud Noise Complaints'::text)
) AS \"count_hotline_noise_12mth\"
FROM boston.\"REF_ZIPCODES\" a;";
// $sql_load_query[24] = "TRUNCATE TABLE boston.\"MASTER_PROPERTY\";

// INSERT INTO boston.\"MASTER_PROPERTY\"
// SELECT A.*, 
// (select COUNT(\"Longitude\") from boston.\"temp_nvcrime_3mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_nvcrime_3mth\".\"Longitude\",boston.\"temp_nvcrime_3mth\".\"Latitude\"))<=0.25
// ) as NVCrime_3mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_nvcrime_6mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_nvcrime_6mth\".\"Longitude\",boston.\"temp_nvcrime_6mth\".\"Latitude\"))<=0.25
// ) as NVCrime_6mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_nvcrime_12mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_nvcrime_12mth\".\"Longitude\",boston.\"temp_nvcrime_12mth\".\"Latitude\"))<=0.25
// ) as NVCrime_12mth_Qtr_Mile,

// (select COUNT(\"Longitude\") from boston.\"temp_vcrime_3mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_vcrime_3mth\".\"Longitude\",boston.\"temp_vcrime_3mth\".\"Latitude\"))<=0.25
// ) as VCrime_3mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_vcrime_6mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_vcrime_6mth\".\"Longitude\",boston.\"temp_vcrime_6mth\".\"Latitude\"))<=0.25
// ) as VCrime_6mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_vcrime_12mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_vcrime_12mth\".\"Longitude\",boston.\"temp_vcrime_12mth\".\"Latitude\"))<=0.25
// ) as VCrime_12mth_Qtr_Mile,

// (select COUNT(\"Longitude\") from boston.\"temp_noise_3mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_noise_3mth\".\"Longitude\",boston.\"temp_noise_3mth\".\"Latitude\"))<=0.25
// ) as Noise_3mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_noise_6mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_noise_6mth\".\"Longitude\",boston.\"temp_noise_6mth\".\"Latitude\"))<=0.25
// ) as Noise_6mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_noise_12mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_noise_12mth\".\"Longitude\",boston.\"temp_noise_12mth\".\"Latitude\"))<=0.25
// ) as Noise_12mth_Qtr_Mile,

// (select COUNT(\"Longitude\") from boston.\"temp_sanitation_3mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_sanitation_3mth\".\"Longitude\",boston.\"temp_sanitation_3mth\".\"Latitude\"))<=0.25
// ) as Sanitation_3mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_sanitation_6mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_sanitation_6mth\".\"Longitude\",boston.\"temp_sanitation_6mth\".\"Latitude\"))<=0.25
// ) as Sanitation_6mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_sanitation_12mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_sanitation_12mth\".\"Longitude\",boston.\"temp_sanitation_12mth\".\"Latitude\"))<=0.25
// ) as Sanitation_12mth_Qtr_Mile,

// (select COUNT(\"Longitude\") from boston.\"temp_housing_3mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_housing_3mth\".\"Longitude\",boston.\"temp_housing_3mth\".\"Latitude\"))<=0.25
// ) as Housing_3mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_housing_6mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_housing_6mth\".\"Longitude\",boston.\"temp_housing_6mth\".\"Latitude\"))<=0.25
// ) as Housing_6mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_housing_12mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_housing_12mth\".\"Longitude\",boston.\"temp_housing_12mth\".\"Latitude\"))<=0.25
// ) as Housing_12mth_Qtr_Mile,

// (select COUNT(\"Longitude\") from boston.\"temp_civic_3mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_civic_3mth\".\"Longitude\",boston.\"temp_civic_3mth\".\"Latitude\"))<=0.25
// ) as Civic_3mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_civic_6mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_civic_6mth\".\"Longitude\",boston.\"temp_civic_6mth\".\"Latitude\"))<=0.25
// ) as Civic_6mth_Qtr_Mile,
// (select COUNT(\"Longitude\") from boston.\"temp_civic_12mth\"
// WHERE (point(A.\"Longitude\",A.\"Latitude\") <@> point(boston.\"temp_civic_12mth\".\"Longitude\",boston.\"temp_civic_12mth\".\"Latitude\"))<=0.25
// ) as Civic_12mth_Qtr_Mile,

// B.JSON_Derulo_Housing,C.JSON_Derulo_Building,D.JSON_Derulo_Code       
// FROM boston.\"STG_PROPERTY\" A
// LEFT JOIN 
// (SELECT \"Latitude\", \"Longitude\", array_to_json(array_agg(\"temp_housing_violation\")) AS JSON_Derulo_Housing FROM boston.\"temp_housing_violation\"
// GROUP BY \"Latitude\", \"Longitude\") AS B
// ON A.\"Latitude\" = B.\"Latitude\"
// AND A.\"Longitude\" = B.\"Longitude\"
// LEFT JOIN 
// (SELECT \"Latitude\", \"Longitude\", array_to_json(array_agg(\"temp_building_violation\")) AS JSON_Derulo_Building FROM boston.\"temp_building_violation\"
// GROUP BY \"Latitude\", \"Longitude\") AS C
// ON A.\"Latitude\" = C.\"Latitude\"
// AND A.\"Longitude\" = C.\"Longitude\"
// LEFT JOIN 
// (SELECT \"Latitude\", \"Longitude\", array_to_json(array_agg(\"temp_code_violation\")) AS JSON_Derulo_Code FROM boston.\"temp_code_violation\"
// GROUP BY \"Latitude\", \"Longitude\") AS D
// ON A.\"Latitude\" = D.\"Latitude\"
// AND A.\"Longitude\" = D.\"Longitude\";";

$sql_load_query[25] = "TRUNCATE TABLE boston.\"MASTER_JSON\";

INSERT INTO boston.\"MASTER_JSON\"
SELECT (select array_to_json(array_agg(distinct temp_nvcrime_12mth)) FROM boston.temp_nvcrime_12mth) AS \"NonViolentCrime_12mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_nvcrime_6mth)) FROM boston.temp_nvcrime_6mth) AS \"NonViolentCrime_6mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_nvcrime_3mth)) FROM boston.temp_nvcrime_3mth) AS \"NonViolentCrime_3mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_vcrime_12mth)) FROM boston.temp_vcrime_12mth) AS \"ViolentCrime_12mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_vcrime_6mth)) FROM boston.temp_vcrime_6mth) AS \"ViolentCrime_6mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_vcrime_3mth)) FROM boston.temp_vcrime_3mth) AS \"ViolentCrime_3mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_noise_12mth)) FROM boston.temp_noise_12mth) AS \"Noise_12mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_noise_6mth)) FROM boston.temp_noise_6mth) AS \"Noise_6mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_noise_3mth)) FROM boston.temp_noise_3mth) AS \"Noise_3mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_sanitation_12mth)) FROM boston.temp_sanitation_12mth) AS \"Sanitation_12mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_sanitation_6mth)) FROM boston.temp_sanitation_6mth) AS \"Sanitation_6mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_sanitation_3mth)) FROM boston.temp_sanitation_3mth) AS \"Sanitation_3mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_housing_12mth)) FROM boston.temp_housing_12mth) AS \"Housing_12mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_housing_6mth)) FROM boston.temp_housing_6mth) AS \"Housing_6mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_housing_3mth)) FROM boston.temp_housing_3mth) AS \"Housing_3mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_civic_12mth)) FROM boston.temp_civic_12mth) AS \"Civic_12mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_civic_6mth)) FROM boston.temp_civic_6mth) AS \"Civic_6mth_JSON\"
     , (select array_to_json(array_agg(distinct temp_civic_3mth)) FROM boston.temp_civic_3mth) AS \"Civic_3mth_JSON\";";


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

