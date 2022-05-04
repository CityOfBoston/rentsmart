<?php
header("Access-Control-Allow-Origin: *");
ini_set('memory_limit', '1024M');
set_time_limit(300);
//echo('v28');
//echo "<br>";
//getting environment variables for database connection
$pg_database = getenv('PG_DATABASE_PROD');
$pg_user = getenv('PG_USER_PROD');
$pg_password = getenv('PG_PASSWORD_PROD');
$pg_host = getenv('PG_HOST_PROD');
$pg_port = getenv('PG_PORT_PROD');
$pg_schema = getenv('PG_SCHEMA_PROD');
//set the connection string
$conn_string = "host={$pg_host} port={$pg_port} dbname={$pg_database} user={$pg_user} password={$pg_password}";
$DBH = new PDO("pgsql:".$conn_string);//connect to DB
//First find address in sam?
$stmt_address_search = $DBH->prepare("SELECT * FROM {$pg_schema}.v_rs_property_main_bldg WHERE UPPER (\"rs_prop_full_address\") = :address OR UPPER (\"rs_prop_full_address\") = :address2 OR UPPER (\"rs_prop_full_address\") = :address3 LIMIT 1;");
if (!$stmt_address_search) {
    echo "\nPDO::errorInfo():\n";
    print_r($DBH->errorInfo());
}
$stmt_address_search_with_zip = $DBH->prepare("SELECT * FROM {$pg_schema}.v_rs_property_main_bldg WHERE (UPPER (\"rs_prop_full_address\") = :address OR UPPER (\"rs_prop_full_address\") = :address2 OR UPPER (\"rs_prop_full_address\") = :address3) AND \"rs_prop_zip_code\" = :zip LIMIT 1;");
if (!$stmt_address_search_with_zip) {
    echo "\nPDO::errorInfo():\n";
    print_r($DBH->errorInfo());
}
//Now some catch situations for stranger addresses or general failures
$stmt_address_range_search_with_zip = $DBH->prepare("SELECT *, 1 as subkey FROM {$pg_schema}.v_rs_property_main_bldg WHERE UPPER (\"rs_prop_full_street_name\") = :streetName AND \"rs_prop_zip_code\" = :zip AND :streetNumber BETWEEN case when regexp_replace(substring(\"rs_prop_street_number\" from 0 for (position('-' in \"rs_prop_street_number\"))), '[^0-9]+', '', 'gi') = '' then regexp_replace(\"rs_prop_street_number\", '[^0-9]+', '', 'gi') else regexp_replace(substring(\"rs_prop_street_number\" from 0 for (position('-' in \"rs_prop_street_number\"))), '[^0-9]+', '', 'gi') END AND regexp_replace(substring(\"rs_prop_street_number\", (position('-' in \"rs_prop_street_number\")+1)), '[^0-9]+', '', 'gi') LIMIT 1;");
if (!$stmt_address_range_search_with_zip) {
    echo "\nPDO::errorInfo():\n";
    print_r($DBH->errorInfo());
}
//stmtaddress range search w/o zip
$stmt_address_range_search = $DBH->prepare("SELECT *, 1 as subkey FROM {$pg_schema}.v_rs_property_main_bldg WHERE UPPER (\"rs_prop_full_street_name\") = :streetName AND :streetNumber BETWEEN case when regexp_replace(substring(\"rs_prop_street_number\" from 0 for (position('-' in \"rs_prop_street_number\"))), '[^0-9]+', '', 'gi') = '' then regexp_replace(\"rs_prop_street_number\", '[^0-9]+', '', 'gi') else regexp_replace(substring(\"rs_prop_street_number\" from 0 for (position('-' in \"rs_prop_street_number\"))), '[^0-9]+', '', 'gi') END AND regexp_replace(substring(\"rs_prop_street_number\", (position('-' in \"rs_prop_street_number\")+1)), '[^0-9]+', '', 'gi') LIMIT 1;");
if (!$stmt_address_range_search) {
    echo "\nPDO::errorInfo():\n";
    print_r($DBH->errorInfo());
}
//catch for random unit types?

//this has to return something different, like just the centered coordinates of the zip code and a flag
$result = 'failure';
	if ($_GET["address"] !== "") {
	$address = strtoupper(str_replace('%20', ' ', $_GET['address']));
	$address = checkSuffix($address);
	if ($_GET["unit"] !== "") {
		$unit = strtoupper(str_replace('%20', ' ', $_GET['unit']));
		$address_to_search = $address.' # '.$unit;
		$address_to_search2 = $address.' #'.$unit;
		$address_to_search3 = $address.' APT '.$unit;
	} else {
		$address_to_search = $address;
	}
	if ($_GET["zip"] !== "" && is_numeric($_GET["zip"])) {
		$stmt_address_search_with_zip->bindParam(':address', $address_to_search, PDO::PARAM_STR);
		$stmt_address_search_with_zip->bindParam(':address2', $address_to_search2, PDO::PARAM_STR);
		$stmt_address_search_with_zip->bindParam(':address3', $address_to_search3, PDO::PARAM_STR);
		$stmt_address_search_with_zip->bindParam(':zip', $_GET["zip"], PDO::PARAM_STR);
		$stmt_address_search_with_zip->execute();
		$result = $stmt_address_search_with_zip->fetchAll(PDO::FETCH_ASSOC);
		if($result){
			echo json_encode($result);
			$DBH = null; //close connection
			exit;
		}else{
			$result = 'none';
		}
	} 
	$stmt_address_search->bindParam(':address', $address_to_search, PDO::PARAM_STR);
	$stmt_address_search->bindParam(':address2', $address_to_search2, PDO::PARAM_STR);
	$stmt_address_search->bindParam(':address3', $address_to_search3, PDO::PARAM_STR);
	$stmt_address_search->execute();
	$result = $stmt_address_search->fetchAll(PDO::FETCH_ASSOC);
		if ($result) {
			echo json_encode($result);
			$DBH = null; //close connection
			exit;
		}else{
			$result = 'none';
		}
	}
 // will always exit if there is a result
//now try with cardinal direction replacements:
	if ($result == 'none') {
	$addressCardDir = checkDirection($address);
	if ($_GET["unit"] !== "") {
		$unit = strtoupper(str_replace('%20', ' ', $_GET['unit']));
		$address_to_search = $addressCardDir.' # '.$unit;
		$address_to_search2 = $addressCardDir.' #'.$unit;
		$address_to_search3 = $addressCardDir.' APT '.$unit;
	} else {
		$address_to_search = $addressCardDir;
	}
	if ($_GET["zip"] !== "" && is_numeric($_GET["zip"])) {
		$stmt_address_search_with_zip->bindParam(':address', $address_to_search, PDO::PARAM_STR);
		$stmt_address_search_with_zip->bindParam(':address2', $address_to_search2, PDO::PARAM_STR);
		$stmt_address_search_with_zip->bindParam(':address3', $address_to_search3, PDO::PARAM_STR);
		$stmt_address_search_with_zip->bindParam(':zip', $_GET["zip"], PDO::PARAM_STR);
		$stmt_address_search_with_zip->execute();
		$result = $stmt_address_search_with_zip->fetchAll(PDO::FETCH_ASSOC);
		if($result){
			echo json_encode($result);
			$DBH = null; //close connection
			exit;
		}else{
			$result = 'none';
		}
	} 
	$stmt_address_search->bindParam(':address', $address_to_search, PDO::PARAM_STR);
	$stmt_address_search->bindParam(':address2', $address_to_search2, PDO::PARAM_STR);
	$stmt_address_search->bindParam(':address3', $address_to_search3, PDO::PARAM_STR);
	$stmt_address_search->execute();
	$result = $stmt_address_search->fetchAll(PDO::FETCH_ASSOC);
		if ($result) {
			echo json_encode($result);
			$DBH = null; //close connection
			exit;
		}else{
			$result = 'none';
		}
	};
//Now go for ranges
if ($result == 'none') { //new failure material goes here
$splitAddress = explode(' ', $address, 2);

if ($_GET["zip"] !== "" && is_numeric($_GET["zip"])) {
$stmt_address_range_search_with_zip->bindParam(':streetName', $splitAddress[1], PDO::PARAM_STR);
$stmt_address_range_search_with_zip->bindParam(':streetNumber', $splitAddress[0], PDO::PARAM_STR);
$stmt_address_range_search_with_zip->bindParam(':zip', $_GET["zip"], PDO::PARAM_STR);
$stmt_address_range_search_with_zip->execute();
$result = $stmt_address_range_search_with_zip->fetchAll(PDO::FETCH_ASSOC);
	if ($result) {
		echo json_encode($result);
		$DBH = null; //close connection
		exit;
	}else{
		$result = 'stillNone';
	}
};
$stmt_address_range_search->bindParam(':streetName', $splitAddress[1], PDO::PARAM_STR);
$stmt_address_range_search->bindParam(':streetNumber', $splitAddress[0], PDO::PARAM_STR);
$stmt_address_range_search->execute();
$result = $stmt_address_range_search->fetchAll(PDO::FETCH_ASSOC);
	if ($result) {
		echo json_encode($result);
		$DBH = null; //close connection
		exit;
	}else{
		$result = 'stillNone';
	}

};//end new failure material for ranges
if ($result == 'stillNone') { //new failure material goes here
$splitAddress = explode(' ', $addressCardDir, 2);

if ($_GET["zip"] !== "" && is_numeric($_GET["zip"])) {
$stmt_address_range_search_with_zip->bindParam(':streetName', $splitAddress[1], PDO::PARAM_STR);
$stmt_address_range_search_with_zip->bindParam(':streetNumber', $splitAddress[0], PDO::PARAM_STR);
$stmt_address_range_search_with_zip->bindParam(':zip', $_GET["zip"], PDO::PARAM_STR);
$stmt_address_range_search_with_zip->execute();
$result = $stmt_address_range_search_with_zip->fetchAll(PDO::FETCH_ASSOC);
	if ($result) {
		echo json_encode($result);
		$DBH = null; //close connection
		exit;
	}else{
		$result = 'stillNone';
	}
};
$stmt_address_range_search->bindParam(':streetName', $splitAddress[1], PDO::PARAM_STR);
$stmt_address_range_search->bindParam(':streetNumber', $splitAddress[0], PDO::PARAM_STR);
$stmt_address_range_search->execute();
$result = $stmt_address_range_search->fetchAll(PDO::FETCH_ASSOC);
	if ($result) {
		echo json_encode($result);
		$DBH = null; //close connection
		exit;
	}else{
		$result = 'stillNone';
	}

};//end new failure material for ranges
$DBH = null; //close connection
if ($result == 'stillNone') { //new failure material goes here
	$arr = array('none' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5);
	echo json_encode($arr);
};
exit;

function checkSuffix($address){
	$replace = array(	'STREET'	=>	'ST',
					 	'AVENUE'	=>	'AVE', 
					 	'BOULEVARD'	=>	'BLVD', 
					 	'CIRCLE'	=>	'CIR',
					 	'COURT'		=>	'CT',
					 	'DRIVE'		=>	'DR',
					 	'HIGHWAY'	=>	'HWY',
					 	'LANE'		=>	'LN',
					 	'PLACE'		=>	'PL',
					 	'PARKWAY'	=>	'PKWY',
					 	'ROAD'		=>	'RD',
					 	'SQUARE'	=>	'SQ',
					 	'TERRACE'	=>	'TER',
					 	'PLAZA'		=>	'PLZ'
				);
foreach ($replace as $input => $replacement) {
	 // The length of the needle as a negative number is where it would appear in the address
    $needle_position = strlen($input) * -1;  
    // If the last N letters match $needle
    if (substr($address, $needle_position) == $input) {
         // Then remove the last N letters from the string
         $address = substr($address, 0, $needle_position);
         $address = $address.$replacement;
    }
}
return $address;
};

function checkDirection($address){
		$replace = array(	'NORTH'	=>	'N',
					 	'SOUTH'	=>	'S', 
					 	'EAST'	=>	'E', 
					 	'WEST'	=>	'W'
				);
$newAddress = strtr($address, $replace);
return $newAddress;
}
?>
