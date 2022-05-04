<?php
set_time_limit(1500);
ini_set('memory_limit', '1024M');
$time_start = microtime(true); //record start time
//Multi-tenancy, available for ALL socrata data-sets
//Put in the endpoint base for the city, the specific dataset code, and the fields wanted
$endpoint_url = 'https://data.cityofboston.gov/resource/'; //City of Boston
$dataset_code = 'qz7u-kb7x'; // Property Assessment 2014 https://data.cityofboston.gov/resource/qz7u-kb7x.json

//Enter the fields you want, setting them equal to their field header in Socrata:
//The key values should be your target database fields.
$fields = array(
"Street_Number"				 		=> 'st_num',				//These 3 will be stitched for full address
"Street_Name"						=> 'st_name',
"Street_Suffix"						=> 'st_name_suf',
"Unit_Number" 						=> 'unit_num',				//The unit number
"Address_Full"   					=> 'full_address',			//Street address given in a single column from the Property Assessment file.  This value might not always match the Address_From_Parts value in a given record due to data quality issues.
"Zipcode" 							=> 'zipcode',		//Zipcode for the address.  Should be given as a 5-digit number, but is stored as a text value to aid combination with the street address.
"Owner" 							=> 'owner',			//Name of the property owner.  This is the only identifying information stored about the owner of a given property.
"Year_Built" 						=> 'yr_built',				//4-digit year for the year of initial construction for a property.  A value of 0 indicates this data was not found in the Property Assessment file for a given record.
"Year_Remodeled" 					=> 'yr_remod',				//4-digit year for the most recent year of renovation for a property.  A value of 0 indicates this data was not found in the Property Assessment file for a given record.
"Location" 							=> 'location',				// Lat and Long, need to be separated
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
$truncate_query = "TRUNCATE {$pg_schema}.\"STG_PROPERTY\""; //Make sure you're truncating the correct table
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

//if ($entry['open_dt'] != '') {
		//Do your customization in here for stitching, separating long/lat, etc
	//Some work on streets, then stitching
	if ($entry['st_name_suf'] == 'AV') {
		$entry['st_name_suf'] = 'AVE';
	}

	if (strpos($entry['full_address'],'#')) {
		$temp = str_replace('#', ' # ', $entry['full_address']);
		$full_Address = str_replace('  ', ' ', $temp);
	}else{
		$full_Address = $entry['full_address'];
	}

	$street_num = $entry['street_num'];
	$street = $entry['street'];
	$street_suf = $entry['st_name_suf'];
	$Address_From_Parts = strtoupper($street_num.' '.$street.' '.$street_suf);
	//Separating Long/Lat, location returns as : (42.340297000, -71.166757000)
	$latitude = substr($entry['location'], 1,12);
	$longitude = substr($entry['location'], 15,13);

	//if (strpos($latitude,'(') === false) {
	if (is_numeric($latitude)) {
//set the insert query
		$insert_query = "INSERT INTO {$pg_schema}.\"STG_PROPERTY\"(\"Street_Address_From_Parts\", \"Unit_Number\", \"Address_Full\", \"Zipcode\", \"Owner\", \"Year_Built\", \"Year_Remodeled\",\"Latitude\", \"Longitude\") VALUES('".$Address_From_Parts."','".$entry['unit_num']."','".$full_Address."','".$entry['zipcode']."','".$entry['owner']."','".$entry['yr_built']."','".$entry['yr_remod']."',".$latitude.",".$longitude.");";
		//perform the insert using pg_query
		$insert_result = pg_query($dbconn, $insert_query);
		if (!$insert_result) {
			echo "An error occurred while running one of the insert queries.\n";
			echo pg_last_error($dbconn);
			exit;
		}
	}}


	//echo("Row # ".$row." :"."<br>");
	$row++;	
//echo("Date: ".$entry["open_dt"]."<br>"."Case: ".$entry["case_title"]."<br>");

//}

$offset = $offset + 10000;
//echo($row."<br>"."<br>".$time."<br>"."<br>");
echo("<br> ROW:  ".$row."<hr>");
}while(count($data)==10000);

// Closing connection
pg_close($dbconn);
echo("Closing the connection");
exit;
?>

