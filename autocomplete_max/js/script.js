// autocomplet : this function will be executed every time we change the text
function autocomplet() {
	var min_length = 6; // min caracters to display the autocomplete
	var keyword = $('#addressTxt').val();
	if (keyword.length >= min_length) {
		$.ajax({
			url: 'autocomplete_max/ajax_refresh.php',
			type: 'POST',
			data: {keyword:keyword},
			success:function(data){
				$('#country_list_id').show();
				$('#country_list_id').html(data);
			}
		});
	} else {
		$('#country_list_id').hide();
	}
}

// set_item : this function will be executed when we select an item
function set_item(item, zip) {
	if (item.indexOf("#") == -1) {
	// change input value
	$('#addressTxt').val(item);
	$('#unitTxt').val('');//Try to set unit to blank if there is no unit (2nd searchof the day)
	$('#zipTxt').val(zip);
	$('#submitBtn').click();
	// hide proposition list
	$('#country_list_id').hide();
	}else{
		address = item.substring(0,(item.indexOf("#")-1));
		unit = item.substr(item.indexOf("#")+1);
	// change input value
	$('#addressTxt').val(address);
	$('#unitTxt').val(unit);
	$('#zipTxt').val(zip);
	$('#submitBtn').click();
	// hide proposition list
	$('#country_list_id').hide();
	};

}