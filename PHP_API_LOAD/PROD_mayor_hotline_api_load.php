<?php
set_time_limit(1500);
ini_set('memory_limit', '1024M');
$time_start = microtime(true); //record start time
//Multi-tenancy, available for ALL socrata data-sets
//Put in the endpoint base for the city, the specific dataset code, and the fields wanted
$endpoint_url = 'https://data.cityofboston.gov/resource/'; //City of Boston
$dataset_code = 'awu8-dc52'; // Mayor's Hotline Data

//Enter the fields you want, setting them equal to their field header in Socrata:
//The key values should be your target database fields.
$fields = array(
"Open_Date" 		=> 'open_dt',				//Date on which the hotline request case was opened.
"Closed_Date" 		=> 'closed_dt',				//Date on which the hotline request case was closed.
"Status"   			=> 'case_status',			//Status of the hotline request case.
"Closure_Reason" 	=> 'closure_reason',		//Reason for the the closure of the hotline request case.
"Case_Title" 		=> 'case_title',			//Individual case type occasionally with appended district or location information.
"Subject" 			=> 'subject',				//Party responsible for case completion
"Reason" 			=> 'reason',				//Overall categorization of case type. Relates to TYPE...
"Type" 				=> 'type',					//Individual case type
"Queue" 			=> 'queue',					//Queue assigned to case. Based on case type and location. ...
"Department" 		=> 'department',			//Department assigned to the case
"Address" 			=> 'location',				//Full address location of the case
"Address_Street" 	=> 'location_street_name',	//Number and street name for the location of the hotline request
"Zipcode" 			=> 'location_zipcode',		//Zipcode for the address.  Should be given as a 5-digit number, but is stored as a text value to aid combination with the street address.
"Property_Type" 	=> 'property_type',			//Property type for the hotline request case.
"Property_ID" 		=> 'property_id',			//ID number for address associated with case. Values uniform with values within CoB Master Address List.
"Latitude" 			=> 'latitude',
"Longitude"			=> 'longitude',
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
$truncate_query = "TRUNCATE {$pg_schema}.\"STG_HOTLINE\""; //Make sure you're truncating the correct table
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

if ($entry['open_dt'] != '000000') {
		
	$open_date = substr($entry['open_dt'], 0, 10);
	$closed_date = $entry['closed_dt'];
	if($closed_date == '000000'){
		$closed_date = '9999-12-31';
	}else{$closed_date = substr($entry['closed_dt'],0,10);}
//set the insert query
		$insert_query = "INSERT INTO {$pg_schema}.\"STG_HOTLINE\"(\"Open_Date\", \"Closed_Date\", \"Status\", \"Closure_Reason\", \"Case_Title\", \"Subject\", \"Reason\", \"Type\", \"Queue\", \"Department\",\"Address\",\"Address_Street\",\"Zipcode\",\"Property_Type\",\"Property_ID\",\"Latitude\", \"Longitude\") VALUES('".$open_date."','".$closed_date."','".$entry['case_status']."','".$entry['closure_reason']."','".$entry['case_title']."','".$entry['subject']."','".$entry['reason']."','".$entry['type']."','".$entry['queue']."','".$entry['department']."','".$entry['location']."','".$entry['location_street_name']."','".$entry['location_zipcode']."','".$entry['property_type']."',".$entry['property_id'].",".$entry['latitude'].",".$entry['longitude'].");";
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

