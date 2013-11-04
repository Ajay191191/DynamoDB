<?
	require 'aws.phar';
	
	use Aws\DynamoDb\DynamoDbClient;
	use Aws\DynamoDb\Model\Attribute;
        use Aws\DynamoDb\Model\Item;


	class dynamoDBClass { 

		//const AWS_SECRET = 'nov9gKt6/z1kh1io2tCJAqw2vqXgngMLTcJv67RQ';
		//const AWS_KEY = 'AKIAJTWBPXRVBSETMKJQ';
		//const AWS_REGION = 'us-west-2';

		public function __construct($AWS_KEY,$AWS_SECRET,$AWS_REGION) {

			$this->client = DynamoDbClient::factory(array(
			    'key'    => $AWS_KEY,
			    'secret' => $AWS_SECRET,
			    'region' => $AWS_REGION
			));
		}

		function createTable ($tableName,$attributeList,$primaryIndexes,$LocalSecondaryIndexes,$read=10,$write=5){


			/*

			Attribute list json: 

			{
				"Attributes":[
					{
						"AttributeName":,
						"AttributeType":
					}
				]
			}
			Primary Index json: 
			{
				"PrimaryIndex" : [
					{
						"AttributeName" : ,
						"KeyType": 
					},
					{
						"AttributeName" : ,
						"KeyType": 
					} 
				]
			}

			Local Secondary index json: 
			{
				"SecondaryIndex":[
					"IndexName": "",
					"KeySchema":[
						{
							"AttributeType" : ,
							"KeyType": 
						},
						{
							"AttributeType" : ,
							"KeyType": 
						}
					],
					"ProjectionType":
				]
			}
                         * 
                         * {
    "Secondary": [
        {
            "SecondaryIndex": {
                "IndexName": "OrderDateIndex",
                "ProjectionType": "KEYS_ONLY",
                "KeySchema": [
                    {
                        "AttributeName": "CustomerID",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "OrderDate",
                        "KeyType": "RANGE"
                    }
                ]
            }
        },
        {
            "SecondaryIndex": {
                "IndexName": "OrderDateIndex",
                "ProjectionType": "KEYS_ONLY",
                "KeySchema": [
                    {
                        "AttributeName": "CustomerID",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "OrderDate",
                        "KeyType": "RANGE"
                    }
                ]
            }
        }
    ]
}
			*/
                        //echo $LocalSecondaryIndexes;
			$primaryIndexes = json_decode($primaryIndexes,true);
			$LocalSecondaryIndexes = json_decode($LocalSecondaryIndexes,true);
			$attributeList = json_decode($attributeList,true);

			$primaryIndexesArray = array();
			$AttributeDefinitionsArray = array();
			$LocalSecondaryIndexesArray = array();

			foreach($attributeList['Attributes'] as $key){
                            array_push($AttributeDefinitionsArray ,array(
				'AttributeName' => $key['AttributeName'],
				'AttributeType' => $key['AttributeType']
                            ));
			}
			foreach($primaryIndexes['PrimaryIndex'] as $key){
				array_push($primaryIndexesArray,array(
                                    'AttributeName' => $key['AttributeName'],
                                    'KeyType' => $key['KeyType']));
			}
			if(!is_null($LocalSecondaryIndexes))
                        foreach($LocalSecondaryIndexes['Secondary'] as $key){
                            
                            //foreach($key['SecondaryIndex'] as $key1)
                            {
                                
                                $Local = array();
                                foreach($key['SecondaryIndex']['KeySchema'] as $key1){
                                    array_push($Local,array(
                                        'AttributeName' => $key1['AttributeName'],
                                        'KeyType' => $key1['KeyType']));
                                }
                                if(isset($key['SecondaryIndex'] ['ProjectionType']) && isset($key['SecondaryIndex'] ['ProjectionAttributes']) )
                                    array_push($LocalSecondaryIndexesArray ,
                                        array(
                                            'IndexName' => $key['SecondaryIndex'] ['IndexName'],
                                            'KeySchema' => $Local,
                                            'Projection' => array(
                                                'ProjectionType' => $key['SecondaryIndex']['ProjectionType'],
                                                'NonKeyAttributes' => $key['SecondaryIndex']['ProjectionAttributes']
                                            ),
                                        )
                                    );
                                elseif(isset($key['SecondaryIndex'] ['ProjectionType']) && !isset($key['SecondaryIndex'] ['ProjectionAttributes']))
                                    array_push($LocalSecondaryIndexesArray ,
                                        array(
                                            'IndexName' => $key['SecondaryIndex'] ['IndexName'],
                                            'KeySchema' => $Local,
                                            'Projection' => array(
                                                'ProjectionType' => $key['SecondaryIndex']['ProjectionType']
                                            ),
                                        )
                                    );
                                elseif(!isset($key['SecondaryIndex'] ['ProjectionType']) && isset($key['SecondaryIndex'] ['ProjectionAttributes']))
                                    array_push($LocalSecondaryIndexesArray ,
                                        array(
                                            'IndexName' => $key['SecondaryIndex'] ['IndexName'],
                                            'KeySchema' => $Local,
                                            'Projection' => array(
                                                'NonKeyAttributes' => $key['SecondaryIndex']['ProjectionAttributes']
                                            ),
                                        )
                                    );
                                else
                                    array_push($LocalSecondaryIndexesArray ,
                                        array(
                                            'IndexName' => $key['SecondaryIndex'] ['IndexName'],
                                            'KeySchema' => $Local,
                                        )
                                    );
                                    
                            }
                        }
                        
                      //  print_r($LocalSecondaryIndexesArray);
                        if(!is_null($LocalSecondaryIndexes))                                
                            $this->client->createTable(array(
                                'TableName' => $tableName,
                                'AttributeDefinitions' => $AttributeDefinitionsArray,
                                'KeySchema' => $primaryIndexesArray,
                                'LocalSecondaryIndexes' => $LocalSecondaryIndexesArray,
                                'ProvisionedThroughput' => array(
        			        'ReadCapacityUnits'  => $read,
        			        'WriteCapacityUnits' => $write
                                 )
                            ));	
                        else
                            $this->client->createTable(array(
                                'TableName' => $tableName,
                                'AttributeDefinitions' => $AttributeDefinitionsArray,
                                'KeySchema' => $primaryIndexesArray,
                                'ProvisionedThroughput' => array(
        			        'ReadCapacityUnits'  => $read,
        			        'WriteCapacityUnits' => $write
                                 )
                            ));	
                            
			$this->client->waitUntilTableExists(array(
			    'TableName' => $tableName
			));
		}

		function updateTable($tableName,$ReadCapacityUnits,$WriteCapacityUnits){
			$this->client->updateTable(array(
			    'TableName' => $tableName,
			    'ProvisionedThroughput' => array(
			        'ReadCapacityUnits'  => $ReadCapacityUnits,
			        'WriteCapacityUnits' => $WriteCapacityUnits
			    )
			));

			// Wait until the table is active again after updating
			$this->client->waitUntilTableExists(array(
			    'TableName' => $tableName
			));
		}


		function getTableDetails($table){
			$result = $this->client->describeTable(array(
			    'TableName' => $table
			));

			// The result of an operation can be used like an array
			//echo $result['Table']['ItemCount'] . "\n";
			//> 0

			// Use the getPath() method to retrieve deeply nested array key values
			//echo $result->getPath('Table/ProvisionedThroughput/ReadCapacityUnits') . "\n";
		}

		function getAllTables(){
			$iterator = $this->client->getIterator('ListTables');

			foreach ($iterator as $tableName) {
			    echo $tableName . "\n";
			}
		}


		function putItem($tableName,$attributes){
			/*
			Attributes json: 
			{
				"Attributes":[
				{
					"Key":,
					"Value":
				}
				]
			}
                         * 
                         */
			$attributes = json_decode($attributes,true);
			$AttributesArray = array();
                        
			foreach($attributes['Attributes'] as $key){
                            if(is_numeric($key['Value'])){
                                $AttributesArray[$key['Key']] =(int)$key['Value'];
                            }
                            else
                                $AttributesArray[$key['Key']] =$key['Value'];
			}
                        
			$result = $this->client->putItem(array(
			    'TableName' => $tableName,
			    'Item' =>$this->client->formatAttributes($AttributesArray),
			    'ReturnConsumedCapacity' => 'TOTAL'
			));
			//echo $result->getPath('ConsumedCapacity/CapacityUnits') . "\n";
		}


		function writeBatch(){
			$result = $client->batchWriteItem(array(
			    'RequestItems' => array(
			        'Orders' => array(
			            array(
			                'PutRequest' => array(
			                    'Item' => array(
			                        'CustomerId' => array('N' => 1041),
			                        'OrderId'    => array('N' => 6),
			                        'OrderDate'  => array('N' => strtotime('-5 days')),
			                        'ItemId'     => array('N' => 25336)
			                    )
			                )
			            ),
			            array(
			                'PutRequest' => array(
			                    'Item' => array(
			                        'CustomerId' => array('N' => 941),
			                        'OrderId'    => array('N' => 8),
			                        'OrderDate'  => array('N' => strtotime('-3 days')),
			                        'ItemId'     => array('N' => 15596)
			                    )
			                )
			            ),
			            array(
			                'PutRequest' => array(
			                    'Item' => array(
			                        'CustomerId' => array('N' => 941),
			                        'OrderId'    => array('N' => 2),
			                        'OrderDate'  => array('N' => strtotime('-12 days')),
			                        'ItemId'     => array('N' => 38449)
			                    )
			                )   
			            ),
			            array(
			                'PutRequest' => array(
			                    'Item' => array(
			                        'CustomerId' => array('N' => 941),
			                        'OrderId'    => array('N' => 3),
			                        'OrderDate'  => array('N' => strtotime('-1 days')),
			                        'ItemId'     => array('N' => 25336)
			                    )
			                )
			            )
			        )
			    )
			));
		}




		function getItem($ConsistentRead,$tableName,$attributeList){
			$attributeList = json_decode($attributeList,true);
			$attributeListArray = array();
			foreach($attributeList['Attributes'] as $key){
                            if(is_numeric($key['Value'])){
                                $attributeListArray[$key['Key']] =array(Attribute::factory((int)$key['Value'])->getType() => (int)$key['Value']);
                            }
                            else
                                $attributeListArray[$key['Key']] =array(Attribute::factory($key['Value'])->getType() => $key['Value']);
			}
                        
                        print_r($attributeListArray);

			$result = $this->client->getItem(array(
			    'ConsistentRead' => (boolean)$ConsistentRead,
			    'TableName' => $tableName,
			    'Key'       => $attributeListArray
			));

			// Grab value from the result object like an array
			print_r($result['Item']);
		}

		function getBatchITems($tableName,$keyVal){
			$tableName = $tableName;
			$keys = array();

			// Given that $keyValues contains a list of your hash and range keys:
			//     array(array(<hash>, <range>), ...)
			// Build the array for the "Keys" parameter
			foreach ($keyValues as $values) {
			    list($hashKeyValue, $rangeKeyValue) = $values;
			    $keys[] = array(
			        'id'   => array('N' => $hashKeyValue),
			        'time' => array('N' => $rangeKeyValue)
			    );
			}

			// Get multiple items by key in a BatchGetItem request
			$result = $client->batchGetItem(array(
			    'RequestItems' => array(
			        $tableName => array(
			            'Keys'           => $keys,
			            'ConsistentRead' => true
			        )
			    )
			));
			$items = $result->getPath("Responses/{$tableName}");
		}

		function query($tableName,$queryJson,$limit=0){

			/*
				Query json: 
				{
					"IndexName":  ,
					"Select": "q",
					"Query": [
					{
						"Key" : "x",
						"Operator" : "y",
						"Value" : "z"
					}
					]
				}
			*/
                        //echo $queryJson;
			$queryJson = json_decode($queryJson,true);
			$queryJsonArray = array();
			foreach($queryJson['Query'] as $key){
                            if(is_numeric($key['Value'])){
                                    $queryJsonArray[$key['Key']] = array(
                                        'AttributeValueList' => array(
                                                array(Attribute::factory((int)$key['Value'])->getType() => (int)$key['Value'])
                                            ),
                                        'ComparisonOperator'=>$key['Operator']
                                     );
                            }
                            else{
                                    $queryJsonArray[$key['Key']] = array(
                                        'AttributeValueList' => array(
                                                array(Attribute::factory($key['Value'])->getType() => $key['Value'])
                                            ),
                                        'ComparisonOperator'=>$key['Operator']
                                     );
                            }
			}
                        
                        //print_r($queryJsonArray);
                        

                        if(isset($queryJson['IndexName']) && isset($queryJson['Select'])){
                           $iterator = $this->client->query(array(
                                'TableName'     => $tableName,
                                'IndexName'     => $queryJson['IndexName'],   // Local Secondary Index.
                                'Select'        => $queryJson['Select'],
                                'limit' => $limit > 0 ?$limit:null,
                                'KeyConditions' => $queryJsonArray
                            )); 
                        }
                        else if(isset($queryJson['Select'])){
                            $iterator = $this->client->query( array(
                                'TableName'     => $tableName,
                                'Select'        => $queryJson['Select'],
                                'limit' => $limit > 0 ?$limit:null,
                                'KeyConditions' => $queryJsonArray
                            ));
                        }
                        else if(isset($queryJson['IndexName'])){
                            $iterator = $this->client->query(array(
                                'TableName'     => $tableName,
                                'IndexName'     => $queryJson['IndexName'],   // Local Secondary Index.
                                'limit' => $limit > 0 ?$limit:null,
                                'KeyConditions' => $queryJsonArray
                            ));
                        }else{
                            $iterator = $this->client->query( array(
                                'TableName'     => $tableName,
                                'limit' => $limit > 0 ?$limit:null,
                                'KeyConditions' => $queryJsonArray
                            ));
                        }

			// Each item will contain the attributes we added
//			foreach ($iterator as $item) {
//			    // Grab the time number value
//			    echo $item['time']['N'] . "\n";
//			    // Grab the error string value
//			    echo $item['error']['S'] . "\n";
//			}
                        return $iterator['Items'];
		}

		function scan($tableName,$jsonscan,$limit=0){
			/*
				Scan Json:
				{
					"Select": "q",
					"ScanFilter":[
					{
						"Key" : "x",
						"Operator" : "y",
						"Value" : "z"
					}
					]
				}
			*/

			$queryJson = json_decode($jsonscan,true);
			$queryJsonArray = array();
			foreach($queryJson['ScanFilter'] as $key){
                            if(is_numeric($key['Value'])){
                                    $queryJsonArray[$key['Key']] = array(
                                        'AttributeValueList' => array(
                                                array(Attribute::factory((int)$key['Value'])->getType() => (int)$key['Value'])
                                            ),
                                        'ComparisonOperator'=>$key['Operator']
                                     );
                            }
                            else{
                                    $queryJsonArray[$key['Key']] = array(
                                        'AttributeValueList' => array(
                                                array(Attribute::factory($key['Value'])->getType() => $key['Value'])
                                            ),
                                        'ComparisonOperator'=>$key['Operator']
                                     );
                            }
			}
                        
                        //print_r($queryJsonArray);
                        $result = array();
                        while(true){
                            if (isset($queryJson['Select'])) {
                                $iterator = $this->client->scan(array(
                                    'TableName' => $tableName,
                                    'Select' => $queryJson['Select'],
                                    'ExclusiveStartKey' => isset($lastKey) ? $lastKey : null,
                                    'limit' => $limit > 0 ? $limit : null,
                                    'ScanFilter' => $queryJsonArray
                                ));
                            } else {
                                $iterator = $this->client->scan(array(
                                    'TableName' => $tableName,
                                    'ExclusiveStartKey' => isset($lastKey) ? $lastKey : null,
                                    'ScanFilter' => $queryJsonArray
                                ));
                            }

                            if ($iterator['Count'] > 0) {
                                array_push($result, array(
                                    'isValid' => 1,
                                    'Result' => isset($queryJson['Select']) && $queryJson['Select'] == 'COUNT' ? $iterator['Count'] : $iterator['Items']
                                ));
                            } else {
                                array_push($result, array(
                                    'isValid' => 0
                                ));
                            }
                            if(isset($iterator['LastEvaluatedKey'])){
                                $lastKey = $iterator['LastEvaluatedKey'];
                            }
                            else
                                break;
                        }
                        return $result;
		}

                /*  Update json: 
                 * 	{
                 * 		"Attributes":[
                 * 		{
                 * 			"Key":,
                 * 			"Value":
                 * 			}
                 *              ]
                 *              "UpdateAttributes" :[
                 *                {
                 *                   "Key":,
                 *                   "Value":,
                 *                   "Action":
                 *                }
                 *              ]
			}
                 */
                function updateItem($tableName,$updateJson){
                    $updateJson = json_decode($updateJson,true);
                    $updateJsonArrayKey = array();
                    $updateJsonArrayAttributes = array();
                    foreach($updateJson['Attributes'] as $key){
                        if(is_numeric($key['Value'])){
                                $updateJsonArrayKey[$key['Key']] = array(
                                        Attribute::factory((int)$key['Value'])->getType()=>(int)$key['Value']
                                    );
                            }
                            else
                                $updateJsonArrayKey[$key['Key']] =array(Attribute::factory($key['Value'])->getType()=>$key['Value']);
                    }
                    foreach($updateJson['UpdateAttributes'] as $key){
                        if(is_numeric($key['Value'])){
                                    $updateJsonArrayAttributes[$key['Key']] = array(
                                        'Value'=>
                                            array(
                                                Attribute::factory((int)$key['Value'])->getType()=>(int)$key['Value']),
                                                 'Action'=>$key['Action']
                                        );
                            }
                            else{
                                    $updateJsonArrayAttributes[$key['Key']] =array('Value'=>array(Attribute::factory($key['Value'])->getType()=>$key['Value']),'Action'=>$key['Action']);
                            }
                    }
                    print_r($updateJsonArrayKey);
                    return $this->client->updateItem(array(
                        'TableName' => $tableName,
                        'Key' => $updateJsonArrayKey,
                        'AttributeUpdates' =>$updateJsonArrayAttributes
                    ));
                }
                
		function deleteItem($tableName,$deleteJson){

			/*
			Delete Json : 
			{
				"Delete":[
				{
					"Key":,
					"Value":
				}
				]
			}
			*/

			$deleteJson = json_decode($deleteJson,true);
			$deleteJsonArray = array();
			foreach($deleteJson['Delete'] as $key){
                                if(is_numeric($key['Value'])){
                                    $deleteJsonArray[$key['Key']] =array(Attribute::factory((int)$key['Value'])->getType() => (int)$key['Value']);
                                }
                                else
                                    $deleteJsonArray[$key['Key']] =array(Attribute::factory($key['Value'])->getType() => $key['Value']);
			}

			//$scan = $this->client->getIterator('Scan', array('TableName' => 'errors'));
			//foreach ($scan as $item) {
			    $this->client->deleteItem(array(
			        'TableName' => $tableName,
			        'Key' => $deleteJsonArray
			    ));
			//}
		}

		function deleteTable($tableName){
			$this->client->deleteTable(array(
			    'TableName' => $tableName
			));

			$this->client->waitUntilTableNotExists(array(
			    'TableName' => $tableName
			));
		}

	}

	//$attrib = Attribute::factory("asd");
	//print_r( $attrib->toArray() );
	//$dynamodb = new dynamoDBInput();
	//$dynamodb -> createTable('Orders, PrimaryIndex : CustomerID as N of HASH;OrderID as S of RANGE, LocalSecondaryIndex : CustomerID as N of HASH;OrderDate as S of RANGE | OrderDateIndex | KEYS_ONLY ');
        //$dynamodb->insert('insert into Orders : CustomerID => 2 ; OrderID => "1" ; OrderDate => 30/06/2013 ; Remarks => Excellent');
        //$dynamodb->insert('errors : id => 2023 ; time => 1372614625 ; error   => Executive overflow ; message => no vacant areas');
        //$dynamodb->select('errors: id => 2023 , true');
         //$dynamodb->query( 'errors : id EQ 1201 ; time GT -15 Days');
        //$dynamodb->delete('errors: id => 2023 ; time => 1372614625');
              
?>