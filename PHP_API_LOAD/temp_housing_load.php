<?php
set_time_limit(1500);
ini_set('memory_limit', '1024M');
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
//read the script file
$sql_load_query = "TRUNCATE TABLE boston.\"temp_housing_violation\";

INSERT INTO boston.\"temp_housing_violation\"
SELECT \"STG_CODE_VIOLATIONS\".\"Latitude\",
    \"STG_CODE_VIOLATIONS\".\"Longitude\",
    \"STG_CODE_VIOLATIONS\".\"Violation_Description\",
    \"STG_CODE_VIOLATIONS\".\"Date\",
    ST_SetSRID(ST_MakePoint(\"STG_CODE_VIOLATIONS\".\"Longitude\", \"STG_CODE_VIOLATIONS\".\"Latitude\"),4326)
   FROM boston.\"STG_CODE_VIOLATIONS\"
  WHERE \"STG_CODE_VIOLATIONS\".\"Enforcement_Type\" = 'Housing'::text";



echo $sql_load_query;
echo "<hr>";
echo "<hr>";
//run the sql load query
echo "Running the database preparation and loading script"; 
echo "<hr>";
$sql_load_result = pg_query($dbconn, $sql_load_query);
if (!$sql_load_result) {
	echo "An error occurred while running the sql load query.\n"; 
	echo pg_last_error($dbconn);
	exit;
}
// Closing connection
pg_close($dbconn);
echo("Closing the connection");
exit;
?>

