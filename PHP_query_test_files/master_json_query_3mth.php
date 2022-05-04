<?php
header("Access-Control-Allow-Origin: *");
ini_set('memory_limit', '1024M');
set_time_limit(300);

//getting environment variables for database connection
$pg_database = getenv('PG_DATABASE_PROD');
$pg_user = getenv('PG_USER_PROD');
$pg_password = getenv('PG_PASSWORD_PROD');
$pg_host = getenv('PG_HOST_PROD');
$pg_port = getenv('PG_PORT_PROD');
$pg_schema = getenv('PG_SCHEMA_PROD');

//set the connection string
$conn_string = "host={$pg_host} port={$pg_port} dbname={$pg_database} user={$pg_user} password={$pg_password}";
// echo "Connecting with connection string: $conn_string";
// echo "<hr>";

//connect to the database
$dbconn = pg_connect($conn_string) or die("Could not connect");
// echo "Successfully connected to database";
// echo "<hr>";

//set the select query
$select_query = "SELECT \"NonViolentCrime_3mth_JSON\", 
       \"ViolentCrime_3mth_JSON\", 
       \"Noise_3mth_JSON\", \"Sanitation_3mth_JSON\", \"Housing_3mth_JSON\", \"Civic_3mth_JSON\" FROM {$pg_schema}.\"MASTER_JSON\";";
$select_result = pg_query($dbconn, $select_query);
if (!$select_result) {
  //echo "An error occurred while running the select query.\n";
  exit;
}

echo json_encode(pg_fetch_row($select_result));

// Closing connection
pg_close($dbconn);
//echo("Closing the connection");
?>