<?php
set_time_limit(1500);
ini_set('memory_limit', '1024M');

function startsWith($haystack, $needle) {
    // search backwards starting from haystack length characters from the end
    return $needle === "" || strrpos($haystack, $needle, -strlen($haystack)) !== FALSE;
}

$time_start = microtime(true); //record start time
//Multi-tenancy, available for ALL socrata data-sets
//Put in the endpoint base for the city, the specific dataset code, and the fields wanted
$endpoint_url = 'https://data.cityofboston.gov/resource/'; //City of Boston
$dataset_code = '8sq6-p7et'; // Code Enforcement Data https://data.cityofboston.gov/resource/8sq6-p7et.json

//Enter the fields you want, setting them equal to their field header in Socrata:
//The key values should be your target database fields.
$fields = array(
"Ticket_Num"				=> 'ticket_no',
"Date" 						=> 'status_dttm',				//
"Status" 					=> 'status',				//
"Violation_Description"   	=> 'description',			//
"Street_Number"			 	=> 'stno',		//
"Strett_Number_High" 		=> 'sthigh',			//
"Street" 					=> 'street',				//
"Street_Suffix" 			=> 'suffix',				//
"City" 						=> 'city',					//
"Zipcode" 					=> 'zip',					//
"Property_ID" 				=> 'property_id',			//
"Latitude" 					=> 'latitude',
"Longitude"					=> 'longitude',

);
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
$truncate_query = "TRUNCATE {$pg_schema}.\"STG_CODE_VIOLATIONS\""; //Make sure you're truncating the correct table
//perform the truncate query
$truncate_result = pg_query($dbconn, $truncate_query);
if (!$truncate_result) {
	echo "An error occurred while running the truncate query.\n";
	echo pg_last_error($dbconn);
	exit;
}
// Now to the loading
echo "Begin loading data array into database line-by-line";
echo "<hr>";
// Loop through each page until count($data)=0 
do
{

$complete_API_url = $endpoint_url . $dataset_code . $format_type . '$select=' . $fields_combined . '&$limit='. $limit . '&$offset=' . $offset;
$data = json_decode((file_get_contents($complete_API_url)), true);
foreach ($data as $entry) {//Go through each entry

	foreach ($fields as $db_field => $field) {//Go through each field wanted
			if(empty($entry[$field])) {
					$entry[$field] = '000000';
			}else{
				$entry[$field] = str_replace($removed_chars, "", $entry[$field]);
			}
	
	}

if ($entry['status_dttm'] != '000000') {
		
	$date = substr($entry['status_dttm'], 0, 10);
	$ticket_num = $entry['ticket_no'];
$enforcement_type = '000000';
	if (startsWith($ticket_num, 'CE')) {
		$enforcement_type = 'Code';
	} 
	if (startsWith($ticket_num, 'HVIOL')) {
		$enforcement_type = 'Housing';
	} 
	if(startsWith($ticket_num, 'V')) {
		$enforcement_type = 'Building';
	}

	
//set the insert query
		//$insert_query = "INSERT INTO {$pg_schema}.\"STG_HOTLINE\"(\"Open_Date\", \"Closed_Date\", \"Status\", \"Closure_Reason\", \"Case_Title\", \"Subject\", \"Reason\", \"Type\", \"Queue\", \"Department\",\"Address\",\"Address_Street\",\"Zipcode\",\"Property_Type\",\"Property_ID\",\"Latitude\", \"Longitude\") VALUES('".$open_date."','".$closed_date."','".$entry['case_status']."','".$entry['closure_reason']."','".$entry['case_title']."','".$entry['subject']."','".$entry['reason']."','".$entry['type']."','".$entry['queue']."','".$entry['department']."','".$entry['location']."','".$entry['location_street_name']."','".$entry['location_zipcode']."','".$entry['property_type']."',".$entry['property_id'].",".$entry['latitude'].",".$entry['longitude'].");";
		$insert_query = "INSERT INTO {$pg_schema}.\"STG_CODE_VIOLATIONS\"(\"Date\", \"Status\", \"Violation_Description\", \"Street_Number\", \"Street_Number_High\", \"Street\", \"Street_Suffix\", \"City\", \"Zipcode\", \"Property_ID\",\"Latitude\", \"Longitude\", \"Enforcement_Type\") VALUES('".$date."','".$entry['status']."','".$entry['description']."','".$entry['stno']."','".$entry['sthigh']."','".$entry['street']."','".$entry['suffix']."','".$entry['city']."','".$entry['zip']."','".$entry['property_id']."',".$entry['latitude'].",".$entry['longitude'].",'".$enforcement_type."');";
		//perform the insert using pg_query
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
}while(count($data)==10000);

// Closing connection
pg_close($dbconn);
echo("Closing the connection");
exit;
?>

