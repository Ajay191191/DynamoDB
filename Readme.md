eDynamo 1.0.1 by Bryan Henry
=======     

DynamoDB Wrapper for PHP


============================================================================================================
Creating a record.
============================================================================================================


	try {
	
		$dynamodb = new eDynamoDB();
		
		// Attributes/Fields can be created on the fly.

		$dynamodb->new_attribute('field_name', 'field-value');
		$dynamodb->put('table_name');

	} catch(Exception $e) {
		echo $e->getMessage();
	} 

        Or for bulk inserting attributes, Create an array : 
        $attributeArray = array(array('fieldname'=>'fieldvalue','fieldname'=>'fieldvalue'),array('fieldname'=>'fieldvalue','fieldname'=>'fieldvalue')); 
        $dynamodb->put('table_name',$attributeArray);


        ex : 
	$attributesArray = array(
                array(
                    'CustomerID' => 234,
                    'OrderID' => 522,
                    'Email' => 'asd@sad.com'
                ),
                array(
                    'CustomerID' => 453,
                    'OrderID' => 332
                )
            );
        $dynamodb->put('table_name',$attributeArray);

        

============================================================================================================
Retrieving records by key.
============================================================================================================


	try {
	
		$dynamodb = new eDynamoDB();
		
		$dynamodb->key('key_name','key_value','rangeKey_name','rangeKey_value'); 
                 //For batch get, set multiple keys.

		$dynamodb->get_attribute('field_name1');
		$dynamodb->get_attribute('field_name2');
		$query = $dynamodb->get('table_name');
 		$row = $query->row();
  		echo $row->field_name1;
  		echo $row->field_name2;

	} catch(Exception $e) {
		echo $e->getMessage();
	} 

============================================================================================================
Scanning a table
============================================================================================================



	try {
	
		$dynamodb = new eDynamoDB();
		
		// Filters are case sensitive.
		$dynamodb->filter('field_name1','operator','value');
		$dynamodb->get_attribute('field_name1');
		$dynamodb->get_attribute('field_name2');
		$query = $dynamodb->scan('table_name');

 		foreach($query->result() as $row) {
  		echo $row->field_name1;
  		echo $row->field_name2;
	 	}

	} catch(Exception $e) {
		echo $e->getMessage();
	} 

    For the list of operators and the accepted values, refer :
    http://docs.aws.amazon.com/aws-sdk-php-2/latest/class-Aws.DynamoDb.DynamoDbClient.html#_scan

    To pass multiple values as values, pass an array.
    Ex: 
    $dynamodb->filter('field_name1','BETWEEN',array(1,100));

============================================================================================================
Updating an item.
============================================================================================================



	try {
	
		$dynamodb = new eDynamoDB();
		 
		$dynamodb->key('key_name','key_value','rangeKey_name','rangeKey_value'); 
		$dynamodb->set('field_name', 'field-value');
		$dynamodb->update('table_name');

	} catch(Exception $e) {
		echo $e->getMessage();
	}
	
	 
============================================================================================================
Deleting an item.
============================================================================================================

	try { 
	
		$dynamodb = new eDynamoDB();
		
		$dynamodb->key('key_name','key_value','rangeKey_name','rangeKey_value'); 
                //For batch delete, set multiple keys.
		$dynamodb->delete('table_name');
	} catch(Exception $e) {
		echo $e->getMessage();
	} 
 
