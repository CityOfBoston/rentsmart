<?php
header("Access-Control-Allow-Origin: *");
ini_set('memory_limit', '1024M');
set_time_limit(3000);
// PDO connect *********

    
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




$keyword = '%'.$_POST['keyword'].'%';
//$keyword = '%216 Florence St%';
//$sql = "SELECT * FROM country WHERE country_name LIKE (:keyword) ORDER BY country_id ASC LIMIT 0, 10";
$sql = $DBH->prepare("SELECT DISTINCT \"rs_sam_full_address\" as \"Address_Full\", \"rs_sam_zip_code\" as \"Zipcode\" FROM {$pg_schema}.\"rs_sam_lkp\" WHERE UPPER(\"rs_sam_full_address\") LIKE UPPER(:keyword) ORDER BY (\"rs_sam_full_address\");");

$sql->bindParam(':keyword', $keyword, PDO::PARAM_STR);
$sql->execute();
$list = $sql->fetchAll();

foreach ($list as $rs) {
	// put in bold the written text
	$country_name = str_ireplace($_POST['keyword'], '<b>'.$_POST['keyword'].'</b>', $rs['Address_Full']);
	$zipcode = $rs['Zipcode'];
	// add new option
    echo '<li onclick="set_item(\''.str_replace("'", "\'", $rs['Address_Full']).'\',\''.str_replace("'", "\'", $rs['Zipcode']).'\')">'.$country_name.'   ,   '.$zipcode.'</li>';
}
?>