<?php


require 'aws.phar';

	use Aws\DynamoDb\DynamoDbClient;
	use Aws\DynamoDb\Model\Attribute;
        use Aws\DynamoDb\Model\Item;

class eDynamoDB {
    /**
	 *
	 * @name Easy DynamoDB
	 * @author Bryan Henry
	 * @version 1.0.1
	 *
	 */

	const AWS_TOOLKIT_PATH = 'aws/';
        const AWS_SECRET = 'zblB2e3L+6N1SMV16CCD0D5sVxsDa0XYcFZyJOIZ';
        const AWS_KEY = 'AKIAJZL24KVRAOAXFEFQ';
        const AWS_REGION = 'us-west-2';

	public function __construct() {
		$this->set_credentials();
		$this->clear_query();
	}


	/**
	 * Establishes AWS credentials.
	 */

	private function set_credentials() {
            
            $this->client = DynamoDbClient::factory(array(
			    'key'    => self::AWS_KEY,
			    'secret' => self::AWS_SECRET,
			    'region' => self::AWS_REGION
			));
	}

	/**
	 *
	 * Establishes filters for a scan.
	 * @param unknown_type $attribute
	 * @param unknown_type $criteria
	 * @throws Exception
	 */

	public function filter($attribute ="", $operator = "" , $criteria ="") {
            
		if(strlen($attribute) == 0) {
			throw new Exception('Filter attribute cannot be blank.');
		}
                if(strlen($operator) == 0) {
			throw new Exception('Filter operator cannot be blank.');
		}

		if(strlen((string)$criteria) == 0) {
                    if($operator == 'NULL' || $operator == 'NOT_NULL'){
                        $this->filter[$attribute] = array(
                            'ComparisonOperator' => $operator
                        );
                    }
                    else
                        throw new Exception('Filter criteria cannot be blank.');
		}
                else{
                    if(sizeof($criteria)>1){
                        $attrib = array();
                        
                        foreach($criteria as $key){
                            
                            array_push($attrib,$this->get_type($key));
                        }
                    }
                    else{
                        $attrib = $this->get_type($criteria);
                    }
                        $attrib = $this->get_type($criteria);
                        $this->filter[$attribute] = array(
                            'ComparisonOperator' => $operator,
                                'AttributeValueList' => array($attrib)
                    );
                    
                }
	}
//        public function filter($attribute ="", $operator = "" , $criteria ="") {
//            
//		if(strlen($attribute) == 0) {
//			throw new Exception('Filter attribute cannot be blank.');
//		}
//                if(strlen($operator) == 0) {
//			throw new Exception('Filter operator cannot be blank.');
//		}
//
//		if(strlen((string)$criteria) == 0) {
//			throw new Exception('Filter criteria cannot be blank.');
//		}
//
//		$attrib = $this->get_type($criteria);
//		$this->filter[$attribute] = array(
//                    'ComparisonOperator' => $operator,
//                    'AttributeValueList' => array($attrib)
//                    );
//	}

	/**
	 *
	 * Selects an attribute to retrieve.
	 * @param string $attribute
	 * @throws Exception
	 */

	public function get_attribute($attribute) {
		if(strlen($attribute) == 0) {
			throw new Exception('You must choose attributes to get');
		}

		$this->attributes[] = (string) $attribute;
	}

	/**
	 *
	 * Prepares an attribute to be inserted.
	 * @param string $attribute
	 * @param unknown_type $value
	 * @throws Exception
	 */

	public function new_attribute($attribute, $value) {

		if(strlen($attribute) == 0) {
			throw new Exception('Put attribute cannot be blank.');
		}

		if(strlen($value) == 0) {
			throw new Exception('Put value cannot be blank.');
		}

		$this->new_attributes[$attribute] = $this->get_type($value);

	}

	/**
	 *
	 * Scans specified table after filters have been established.
	 * @param unknown_type $table
	 * @throws Exception
	 */

	public function scan($table) {

		$all_keys = 0;
		$res = array();
		$query_results = array();
		while(!$all_keys) {

                    if(sizeof($this->attributes) != 0) {
			$query = array('TableName'       	 => $table,
				  	   	   'AttributesToGet' 	 => $this->attributes
			);
                    }
                    else{
                        $query = array('TableName'=> $table);
                    }

                    if(isset($this->select)){
                        $query['Select'] = $this->select;
                    }
                    if(isset($this->indexName)){
                        $query['IndexName'] = $this->indexName;        
                    }
                    if(sizeof($this->filter) > 0) {
                        $query['ScanFilter'] = $this->filter;
                    }		
			
                    if(isset($last_key)) {
                        $query['ExclusiveStartKey'] = $last_key;
                    }
                    
                    if(isset($this->limit) && $this->limit > 0) {
                        $query['Limit'] = (int) $this->limit;
                    }
                    
                    $items = $this->client->scan($query);
                    
                    if(is_array($items['Items'])) {
                        $result = call_user_func_array('array_merge_recursive', $items['Items']);
                        
                        $res = array_merge_recursive($res, $items['Items']);
                    }
                    $this->row_count += $items['Count'];
                    if(isset($items['LastEvaluatedKey'])){
                        $last_key = $items['LastEvaluatedKey'];        
                    }
                    else{
                        $all_keys = true;    
                    }
		}

		$query = new eDynamoQuery( array('num_rows' => $this->row_count, 'results' => $res));
		return $query;
		$this->clear_query();
	}


	/**
	 * Executes the query after attributes and key(s) have been selected.
	 * @param unknown_type $table
	 */

	public function get($table) {

		if (sizeof($this->attributes) == 0) {
			throw new Exception('No attributes were selected.');
		}

		if(strlen($table) == 0) {
			throw new Exception('No table was selected.');
		}

		if(!isset($this->key)) {
			throw new Exception('No key was selected.');
		}


		if(!isset($this->keys)) {
                        
                        
			$response = $this->client->getItem(array('TableName' => $table, 'Key' => $this->key));
				
			if(!isset($response['Item'])) {
				$count = 0;
			} else {
				$count = 1;
			}
			$query = new eDynamoQuery( array('num_rows' => $count, 'results' => $response['Item']));
			$this->clear_query();
		} else {
                    foreach($this->keys as $key_value) {
                        $key_push = $key_value;
                        $keys[] = $key_push;
                    }

				
			$response = $this->client->batchGetItem(array('RequestItems' => array($table => array('Keys' => $keys ))));
				
			$items = $response['Responses'];
                        
                        print_r(array('num_rows' => $this->row_count, 'results' => $items[$table]['Items']));
			$query = new eDynamoQuery( array('num_rows' => $this->row_count, 'results' => $items[$table]['Items']));
			$this->clear_query();
		}

		return $query;
                //return $response;
	}
        
        /**
	 * Executes the query after attributes and key(s) have been selected.
	 * @param unknown_type $table
	 */
        public function query($table) {

		if (sizeof($this->attributes) == 0) {
			throw new Exception('No attributes were selected.');
		}

		if(strlen($table) == 0) {
			throw new Exception('No table was selected.');
		}

		if(!isset($this->key)) {
			throw new Exception('No key was selected.');
		}


		if(!isset($this->keys)) {
                        $query = array('TableName' => $table, 'Key' => $this->key);
                        
                        if(isset($this->indexName)){
                            $query['IndexName'] = $this->indexName;
                        }
                        if(isset($this->select)){
                            $query['Select'] = $this->select;
                        }
                    
			$response = $this->client->getItem($query);
			if(!isset($response['Item'])) {
				$count = 0;
			} else {
				$count = 1;
			}
			$query = new eDynamoQuery( array('num_rows' => $count, 'results' => $response['Item']));
			$this->clear_query();

		} else {

                    foreach($this->keys as $key_value) {
                        $key_push = $key_value;
                        $keys[] = $key_push;
                    }

				
			$response = $this->client->batchGetItem(array('RequestItems' => array($table => array('Keys' => $keys ))));
				
			$items = $response['Responses'];

			$query = new eDynamoQuery( array('num_rows' => $this->row_count, 'results' => $items[$table]['Items']));
			$this->clear_query();
		}

		return $query;
                //return $response;
	}

	/**
	 *
	 * Sets the query key(s) and the range(s)
	 * @param unknown_type $key
	 * @param unknown_type $range
	 * @throws Exception
	 */
	public function key($key,$value, $rangeKey="",$rangeValue="") {

		if(!isset($key)) {
			throw new Exception('No key was selected.');
		}

		if(!isset($this->key)) {
                        $this->key = array($key => $this->get_type($value));
			if(!empty($rangeKey)) {
				$this->key[$rangeKey] = $this->get_type($rangeValue);
			}
		} else {
			if(!isset($this->keys)) {
				$this->keys[] = $this->key;
			}

			$key_push[$key] = $this->get_type($value);

			if(!empty($rangeKey)) {
				$key_push[$rangeKey] = $this->get_type($rangeValue);
			}

			$this->keys[] = $key_push;

		}
	}
        
        
        /**
	 *
	 * Sets the query on a local secondary index
	 * @param unknown_type $indexName
	 * @throws Exception
	 */
        public function setIndexName($indexName){
            if(strlen($indexName)==0){
                throw new Exception("Index Name cannot be null");
            }
            
            $this->indexName = $indexName;
            
        }
        
                
        /**
	 *
	 * Sets the Select parameter for query
	 * @param unknown_type $select
	 * @throws Exception
	 */
        public function select($select){
            if(strlen($select)==0){
                throw new Exception("Select cannot be null");
            }
            
            $this->select = $select;
        }


	/**
	 *
	 * Inserts the defined attributes.
	 * @param unknown_type $table
	 * @throws Exception
	 */
	public function put($table, $insert_array ="") {

		if(strlen($table) == 0) {
			throw new Exception('No table was selected.');
		}


		if(!is_array($insert_array)) {

			if(!isset($this->new_attributes)) {
				throw new Exception('Must define attributes to put.');
			}

			$query = array('TableName' => $table,'Item'=> $this->new_attributes);

			print_r($query);
                        print_r($this->new_attributes);
			$this->clear_query();
                        $put_response = $this->client->putItem($query);

		} elseif(is_array($insert_array)) {
			if(sizeof($insert_array) == 0) {
				throw new Exception('Batch array must be populated');
			}

			foreach($insert_array as $row) {
				$Items['RequestItems'][$table][]['PutRequest'] = array('Item' => Attribute::factory($row));
			}
				
			$response = $this->client->batchWriteItem($Items);
		}
	}


	/**
	 *
	 * Establishes item limits for scans.
	 * @param integer $limit
	 * @throws Exception
	 */
	public function limit($limit) {
		if(!is_numeric($limit)) {
			throw new Exception('Limits must be numeric');
		}

		$this->limit = $limit;

	}

	/**
	 *
	 * Deletes items as defined by their key.
	 * @param unknown_type $table
	 * @throws Exception
	 */
	public function delete($table) {

		if(strlen($table) == 0) {
			throw new Exception('No table was selected.');
		}

		if(!isset($this->key)) {
			throw new Exception('No key was selected.');
		}

		$this->client->deleteItem(array('TableName' => $table, 'Key' => $this->key));
		$this->clear_query();
	}

	/**
	 *
	 * Sets attribute value for update query.
	 * @param unknown_type $attribute
	 * @param unknown_type $value
	 */
	public function set($attribute, $value) {
		$this->set[$attribute] = array( 'Action' => 'PUT', 'Value' => $this->get_type($value));
	}

	public function set_add($attribute, $value) {
		$this->set[$attribute] = array( 'Action' => 'ADD', 'Value' => $this->get_type($value));
	}


	/**
	 *
	 * Updates the supplied key.
	 * @param unknown_type $table
	 * @throws Exception
	 */
	public function update($table) {

		if(strlen($table) == 0) {
			throw new Exception('No table was selected.');
		}

		if(!isset($this->key)) {
			throw new Exception('No key was selected.');
		}
                
                $response = $this->client->updateItem(array('TableName' => $table, 'Key' => $this->key, 'AttributeUpdates' => $this->set ));
			
		$this->clear_query();
	}

	/**
	 * Clears the last query parameters for object reuse..
	 */

	private function clear_query() {
		$this->set = null;
		$this->key = null;
		$this->keys = null;
		$this->row_count = 0;
		$this->limit = null;
		$this->new_attributes = null;
		$this->attributes = array();
		$this->filter = array();
                $this->indexName = null;
                $this->select = null;
	}

	/**
	 *
	 * Determines the attribute/key type.
	 * @param unknown_type $criteria
	 * @throws Exception
	 */
	private function get_type($criteria) {

		if(!isset($criteria) || strlen($criteria) == 0) {
			throw new Exception('Put value cannot be blank.');
		}

		//return $this->db->attribute($criteria);
                return Attribute::factory($criteria);
	}

}

class eDynamoQuery {

	/*
	 * eDynamoQuery
	 * Bryan Henry
	 * 9/14/2012
	 *
	 */


	public function __construct($params) {
		$this->num_rows = (int) $params['num_rows'];
		$this->results  = $params['results'];
                
	}

	/**
	 *
	 * Returns the number of rows
	 * @return integer
	 *
	 */
	public function num_rows() {
		return $this->num_rows;
	}

	/**
	 *
	 * Returns a simple object with the first row of data.
	 * @return object
	 */
	public function row() {
                print_r($this->results);
		if($this->num_rows > 0) {
			$item_obj = new stdClass();
			foreach($this->results as $key => $value) {
				$item_obj->{$key} = array_pop($value);
			}
			return $item_obj;
		} else {
			return false;
		}
	}

	/**
	 * Iterates through the results and returns an array of objects.
	 * @return array
	 */
	public function result() {
		if($this->num_rows > 1 && sizeof($this->results)>0) {
			foreach($this->results as $obj) {
				$item_obj = new stdClass();
				foreach($obj as $key => $value) {
					$item_obj->{$key} = array_pop($value);
				}
				$result[] = $item_obj;
			}
			return $result;
		} elseif($this->num_rows == 1) {
			return array($this->row());
		} else {
			return array();
		}
	}
        
        function orderBy($attribute, $order) {
            $price = array();
            foreach ($this->results as $key => $row) {
                $price[$key] = $row[$attribute];
            }
            if ($order == 'asc')
                array_multisort($price, SORT_ASC, $this->results);
            if ($order == 'desc')
                array_multisort($price, SORT_DESC, $this->results);
        }
        
}

$dynamodb = new eDynamoDB();
//Insert item
$attributes = array();

$dynamodb->new_attribute('CustomerID', 3);
$dynamodb->new_attribute('OrderID', 4);
$dynamodb->put('Orders');


//retrieve
//$dynamodb->key('CustomerID',3,'OrderID',3);
//$dynamodb->get_attribute('CustomerID');
//$query = $dynamodb->get('Orders');
//
//$row = $query->row();
//echo $row->CustomerID;
//echo $row->field_name2;

//Scan
//$dynamodb->filter("CustomerID",'EQ',3);
//$dynamodb->get_attribute('CustomerID');
//$dynamodb->select('COUNT');
//$query = $dynamodb->scan('Orders');

//$query->orderBy('CustomerID', 'desc');

//echo $query->num_rows();
//print_r($query->results);
//foreach ($query->result() as $row) {
//    print_r($row);
//}

//Update
//print_r($query['Items']);
//$dynamodb = new eDynamoDB();
//$dynamodb->key('CustomerID',2,'OrderID',2);
//$dynamodb->set('Email', 'asd@loj.com');
//$dynamodb->update('Orders');


//Delete
///$dynamodb = new eDynamoDB();
//$dynamodb->key('CustomerID',2,'OrderID',2);
//$dynamodb->delete('Orders');
//foreach ($query->result() as $row) {
//    echo $row->field_name1;
//    echo $row->field_name2;
//}
?>