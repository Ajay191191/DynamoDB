<?php

include('dynamo.php');
require 'aws.phar';
	
	use Aws\DynamoDb\DynamoDbClient;
	use Aws\DynamoDb\Model\Attribute;
        use Aws\DynamoDb\Model\Item;


class dynamoDBInput {

    function __construct($AWS_KEY, $AWS_SECRET, $AWS_REGION) {
        $this->dynamodbClass = new dynamoDBClass($AWS_KEY, $AWS_SECRET, $AWS_REGION);
    }

    function setUnits($read, $write) {
        $this->read = $read;
        $this->write = $write;
    }
    
    function setSelect($val) {
        $this->select = $val;
    }

    function setIndexName($val) {
        $this->indexName = $val;
    }
    
    
    public function new_attribute($attribute, $value) {

        if (strlen($attribute) == 0) {
            throw new Exception('Put attribute cannot be blank.');
        }

        if (strlen($value) == 0) {
            throw new Exception('Put value cannot be blank.');
        }

        $this->new_attributes[$attribute] = array(
                       Attribute::factory($value)->getType() => $value
                );
    }
    
    /**
     *
     * Inserts the defined attributes.
     * @param unknown_type $table
     * @throws Exception
     */
    public function put($table, $insert_array = "") {

        if (strlen($table) == 0) {
            throw new Exception('No table was selected.');
        }


        if (!is_array($insert_array)) {

            if (!isset($this->new_attributes)) {
                throw new Exception('Must define attributes to put.');
            }

            $query = array('TableName' => $table,
                'Item' => $this->new_attributes);

            //$put_response = $this->db->put_item($query);
            $result = $this->client->putItem($query);

            $this->clear_query();
        } elseif (is_array($insert_array)) {
            if (sizeof($insert_array) == 0) {
                throw new Exception('Batch array must be populated');
            }

            foreach ($insert_array as $row) {
                $Items['RequestItems'][$table][]['PutRequest'] = array('Item' => $this->db->attributes($row));
            }

            $response = $this->db->batch_write_item($Items);
            if (!$response->isOK()) {
                throw new Exception($response->body->message);
            }
        }
    }
    
    public function key($key, $range = "") {

        if (!isset($key)) {
            throw new Exception('No key was selected.');
        }

        if (!isset($this->key)) {
            $this->key = array('hash_key' => $this->get_type($key));
            if (!empty($range)) {
                $this->key['range_key'] = $this->get_type($range);
            }
        } else {
            if (!isset($this->keys)) {
                $this->keys[] = $this->key;
            }

            $key_push['hash_key'] = $this->get_type($key);

            if (!empty($range)) {
                $key_push['range_key'] = $this->get_type($range);
            }

            $this->keys[] = $key_push;
        }
    }

    function orderBy($result, $attribute, $order) {
        $price = array();
        foreach ($result as $key => $row) {
            $price[$key] = $row[$attribute];
        }
        if ($order == 'asc')
            array_multisort($price, SORT_ASC, $result);
        if ($order == 'desc')
            array_multisort($price, SORT_DESC, $result);
        return $result;
    }
    
    function clear(){
        
        $this->result=null;
        $this->condition=null;
        $this->select=null;
        $this->read=null;
        $this->write=null;
        $this->indexName = null;
    }
}
?>
