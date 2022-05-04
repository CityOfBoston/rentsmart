<?php
set_time_limit(1500);
ini_set('memory_limit', '1024M');
$time_start = microtime(true); //record start time
//Multi-tenancy, available for ALL socrata data-sets
//Put in the endpoint base for the city, the specific dataset code, and the fields wanted
$endpoint_url = 'https://data.cityofboston.gov/resource/'; //City of Boston
$dataset_code = 'juwp-xmns'; // Crime Incidents https://data.cityofboston.gov/resource/7cdf-6fgx.json
//https://data.cityofboston.gov/resource/juwp-xmns.json
//Enter the fields you want, setting them equal to their field header in Socrata:
//The key values should be your target database fields.
$fields = array(
"Nature_Code" 				=> 'offense_code',				//
"Incident_Description" 		=> 'name',	//		//
"Date" 						=> 'occurred_on',					//
"Latitude" 					=> 'lat',
"Longitude"					=> 'long',
"Crime_Part"				=> 'ucr_part',
);//Need to separate location and fix date
//Automatic settings: API Builder
$format_type = '.json?';
$fields_combined = implode(",", $fields);
//Loop through each page:
$limit = 10000;
$offset = 0;	
$row = 1;
$removed_chars = array("'","\""); //set list of characters to reomve from strings to prevent data loading issues
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
//Truncate the target table
//set the truncate query


// $truncate_query = "TRUNCATE {$pg_schema}.\"STG_CRIME\""; //Make sure you're truncating the correct table
// //perform the truncate query
// $truncate_result = pg_query($dbconn, $truncate_query);
// if (!$truncate_result) {
// 	echo "An error occurred while running the truncate query.\n";
// 	echo pg_last_error($dbconn);
// 	exit;
// }


// Now to the loading
echo "Begin loading data array into database line-by-line";
echo "<hr>";
// Loop through each page until count($data)=0 
//do
//{

//$complete_API_url = $endpoint_url . $dataset_code . $format_type . '$select=' . $fields_combined . '&$limit='. $limit . '&$offset=' . $offset;
//echo($complete_API_url);
$complete_API_url = 'CRIME_INCIDENT_TEST_real.json';
$data = json_decode((file_get_contents($complete_API_url)), true);
foreach ($data as $entry) {//Go through each entry

	foreach ($fields as $db_field => $field) {//Go through each field wanted
			if(empty($entry[$field])) {
					$entry[$field] = '000000';
			}else{
				$entry[$field] = str_replace($removed_chars, "", $entry[$field]);
			}
	
	}

if ($entry['ocurred_on'] != '000000') {
		
	//$date = substr($entry['fromdate'], 0, 10);
	//$latitude = $entry['location']['latitude'];
	//$longitude = $entry['location']['longitude'];
	$latitude = $entry['bpdlat'];
	$longitude = $entry['bpdlong'];
	$date = '2016-05-16';
	$UCRpart = 'Part Two';
	
//set the insert query
		$insert_query = "INSERT INTO {$pg_schema}.\"STG_CRIME\"(\"Nature_Code\", \"Incident_Description\", \"Crime_Code\", \"Date\", \"Latitude\", \"Longitude\", \"Crime_Part\") VALUES('".$entry['offense_code']."','".$entry['name']."','".$entry['offense_code']."','".$date."',".$latitude.",".$longitude.",'".$UCRpart."');";
		//perform the insert using pg_query
		//TESTING
		echo "<hr>";
echo($latitude);
echo "<hr>";
echo($longitude);
echo "<hr>";
echo($date);
echo "<hr>";
echo($UCRpart);
echo "<hr>";
echo($entry['offense_code']);
echo "<hr>";
echo($entry['name']);
echo "<hr>";
echo($insert_query);
echo "<hr>";
		//TESTING
		$insert_result = pg_query($dbconn, $insert_query);
		if (!$insert_result) {
			echo "An error occurred while running one of the insert queries.\n";
			echo pg_last_error($dbconn);
			exit;
		}
	}


	//echo("Row # ".$row." :"."<br>");
	$row++;	
//echo("Date: ".$entry["open_dt"]."<br>"."Case: ".$entry["case_title"]."<br>");

}

$offset = $offset + 10000;
//echo($row."<br>"."<br>".$time."<br>"."<br>");
echo("<br> ROW:  ".$row."<hr>");
//}while(count($data)==10000);

// Closing connection
pg_close($dbconn);
echo("Closing the connection");
exit;
?>

