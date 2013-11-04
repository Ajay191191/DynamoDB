<?php

include('dynamo.php');
require_once('php-sql-parser.php');
require_once('php-sql-creator.php');

class dynamoDBInput {

    function __construct($AWS_KEY, $AWS_SECRET, $AWS_REGION) {
        $this->dynamodbClass = new dynamoDBClass($AWS_KEY, $AWS_SECRET, $AWS_REGION);
    }

    function setUnits($read, $write) {
        $this->read = $read;
        $this->write = $write;
    }

    //Ex: createTable('TableName, PrimaryIndex : X as T of H;Y as T of R',' LocalSecondaryIndex : { X as T of R;Y as T of H | IndexName | ProjectionType | NonKeyAttributes } { X as T of R;Y as T of H | IndexName | ProjectionType | NonKeyAttributes}');
    function createTable($table, $localSecIndex = null) {
        $primaryIndexes = (object) null;
        $LocalSecondaryIndexes = (object) null;
        $Attributes = (object) null;
        $AllIndexes = explode(",", $table);
        $tableName = $AllIndexes[0];

        $temp = explode(":", $AllIndexes[1]);
        $primaryIndex = explode(";", $temp[1]);




        $primaryJson = "{\"PrimaryIndex\":[";

        $AttributeJson1 = "{\"Attributes\":[";
        $AttributeJson2 = "{\"Attributes\":[";

        for ($i = 0; $i < sizeof($primaryIndex); $i++) {
            $Attrib = preg_split("/\b(as)\b/i", $primaryIndex[$i]);
            $primaryIndexes->AttributeName = trim($Attrib[0]);
            $keyType = preg_split("/\b(of)\b/i", $Attrib[1]);
            $primaryIndexes->KeyType = trim($keyType[1]);
            $Attributes->AttributeName = trim($Attrib[0]);
            $Attributes->AttributeType = trim($keyType[0]);
            if ($i < sizeof($primaryIndex) - 1) {
                $primaryJson .= json_encode($primaryIndexes) . ",";
                $AttributeJson1 .= json_encode($Attributes) . ",";
            } else {
                $primaryJson .= json_encode($primaryIndexes);
                $AttributeJson1 .= json_encode($Attributes);
            }
        }
        $primaryJson .= "]}";
        $AttributeJson1.= "]}";
        $secJson = "{\"Secondary\":[";
        //print_r($primaryIndex);
        if (!is_null($localSecIndex)) {


            //$regex = '"/{.*?}/"';
            //$regex = '/{([^{}]+)}/';
            $regex = '/\{([^\}]*)\}/';
            $temp = explode(":", $localSecIndex);

            if (preg_match_all($regex, $temp[1], $matches)) {

                for ($j = 0; $j < sizeof($matches[1]); $j++) {
                    $LocalSecondaryIndexes = (object) null;
                    $secondaryJson = "{\"SecondaryIndex\":{";

                    $matches[1][$j] = trim($matches[1][$j], "{}");
                    $temp = explode("|", $matches[1][$j]);
                    ;
                    $LocalSecondaryIndex = explode(";", $temp[0]);



                    $LocalSecondaryIndexes->IndexName = trim($temp[1]);
                    $LocalSecondaryIndexes->ProjectionType = trim($temp[2]);
                    if (isset($temp[3])) {
                        $att = str_replace("[", "", $temp[3]);
                        $att = str_replace("]", "", $att);
                        $att = explode(",", $att);
                        $fAtt = array();
                        for ($attI = 0; $attI < sizeof($att); $attI++) {
                            array_push($fAtt, trim($att[$attI]));
                        }
                        $LocalSecondaryIndexes->ProjectionAttributes = $fAtt;
                    }
                    $str = str_replace("{", "", json_encode($LocalSecondaryIndexes));
                    $str = str_replace("}", "", $str);
                    $secondaryJson .= trim($str) . ",";
                    $secondaryJson .= "\"KeySchema\":[";
                    $LocalSecondaryIndexes = (object) null;
                    $Attributes = (object) null;
                    for ($i = 0; $i < sizeof($LocalSecondaryIndex); $i++) {
                        $Attrib = preg_split("/\b(as)\b/i", $LocalSecondaryIndex[$i]);
                        $LocalSecondaryIndexes->AttributeName = trim($Attrib[0]);
                        $keyType = preg_split("/\b(of)\b/i", $Attrib[1]);

                        $LocalSecondaryIndexes->KeyType = trim($keyType[1]);


                        $Attributes->AttributeName = trim($Attrib[0]);

                        $Attributes->AttributeType = trim($keyType[0]);




                        if ($i < sizeof($LocalSecondaryIndex) - 1) {
                            $secondaryJson .= json_encode($LocalSecondaryIndexes) . ",";
                        } else {
                            $secondaryJson .= json_encode($LocalSecondaryIndexes);
                        }
                        if ($j < sizeof($matches[1]) - 1 || $i < sizeof($LocalSecondaryIndex) - 1) {
                            $AttributeJson2 .= json_encode($Attributes) . ",";
                        } else {
                            $AttributeJson2 .= json_encode($Attributes);
                        }
                    }
                    $secondaryJson .= "]}}";

                    if ($j < sizeof($matches[1]) - 1)
                        $secJson .= $secondaryJson . ",";
                    else
                        $secJson .= $secondaryJson;
                }
                $AttributeJson2.= "]}";
            }
            $secJson .= "]}";

            $AttributesList = $this->combineJson($AttributeJson1, $AttributeJson2);
        }

        if (!isset($AttributesList)) {
            $AttributesList = $AttributeJson1;
        }
        //echo $secJson;
        
        $this->dynamodbClass->createTable($tableName, $AttributesList, $primaryJson, $secJson, $this->read, $this->write);
        $this->clear();
    }

    function combineJson($json1, $json2) {


        $output = (object) null;
        $jsonOutput = "{\"Attributes\":[";

        $jsonOutputPB = $json1;
        $jsonPB = json_decode($jsonOutputPB, TRUE);

        $jsonArrayPB = $jsonPB['Attributes'];

        $Attributes = array();

        $jsonOutputRM = $json2;
        $jsonRM = json_decode($jsonOutputRM, TRUE);

        $jsonArrayRM = $jsonRM['Attributes'];

        $i = 0;
        foreach ($jsonRM['Attributes'] as $key) {
            if (!$this->contains($jsonArrayPB, "AttributeName", $key['AttributeName']) && !in_array($key['AttributeName'], $Attributes)) {
                array_push($Attributes, $key['AttributeName']);
                $output->AttributeName = $key['AttributeName'];
                $output->AttributeType = $key['AttributeType'];
                if ($i++ < sizeof($jsonArrayRM) - 1)
                    $jsonOutput .= json_encode($output) . ",";
                else
                    $jsonOutput .= json_encode($output);
            }
        }
        $i = 0;
        foreach ($jsonPB['Attributes'] as $key) {
            if (!in_array($key['AttributeName'], $Attributes)) {
                array_push($Attributes, $key['AttributeName']);
                $output->AttributeName = $key['AttributeName'];
                $output->AttributeType = $key['AttributeType'];
                if ($i++ < sizeof($jsonArrayPB) - 1)
                    $jsonOutput .= json_encode($output) . ",";
                else
                    $jsonOutput .= json_encode($output);
            }
        }
        $jsonOutput .= "]}";
        return($jsonOutput);
    }

    function contains($array, $key, $val) {
        foreach ($array as $item) {
            if (isset($item[$key]) && trim($item[$key]) == trim($val)) {
                return true;
            }
        }
        return false;
    }

    /* Insert
     * 
     * insert(TableName : Key => Value ; Key => Value );
     */

    function insert($query) {
        $temp = explode(":", $query);

        $tableName = explode(" ", trim($temp[0]));
        $tableName = trim($tableName[2]);

        $AttributesJson = (object) null;
        $Attributes = "{\"Attributes\":[";
        $temp = explode(";", $temp[1]);
        for ($i = 0; $i < sizeof($temp); $i++) {
            $attr = explode("=>", $temp[$i]);
            $AttributesJson->Key = trim($attr[0]);
            $AttributesJson->Value = trim($attr[1]);
            if ($i < sizeof($temp) - 1) {
                $Attributes .= json_encode($AttributesJson) . ",";
            } else {
                $Attributes .= json_encode($AttributesJson);
            }
        }
        $Attributes .= "]}";
        $this->dynamodbClass->putItem($tableName, $Attributes);
        $this->clear();
    }

    /*
     * Select One Item
     * select( TableName : HashKey => Value ;  RangeKey => Value , hasConsistentRead );
     */

    function select($query) {
        $temp = explode(":", $query);
        $tableName = trim($temp[0]);
        $AttributesJson = (object) null;
        $Attributes = "{\"Attributes\":[";
        $temp = explode(",", $temp[1]);
        $consistentRead = trim($temp[1]);
        $temp = explode(";", $temp[0]);
        for ($i = 0; $i < sizeof($temp); $i++) {
            $attr = explode("=>", $temp[$i]);
            $AttributesJson->Key = trim($attr[0]);
            $AttributesJson->Value = trim($attr[1]);
            if ($i < sizeof($temp) - 1) {
                $Attributes .= json_encode($AttributesJson) . ",";
            } else {
                $Attributes .= json_encode($AttributesJson);
            }
        }
        $Attributes .= "]}";
        $this->dynamodbClass->getItem($consistentRead, $tableName, $Attributes);
        $this->clear();
    }

    /*
     *  Set IndexName to query on a secondary index.
     *  Select: ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT .
     *  
     *  Value : Contains exactly one value, except for a BETWEEN or IN comparison, in which case it contains two values.
     *  Op :  EQ, NE, IN, LE, LT, GE, GT, BETWEEN, NOT_NULL, NULL, CONTAINS, NOT_CONTAINS, BEGINS_WITH. 
     *  query ( TableName : key Op Value ; Key Op Value)
     *  Ex. query( errors : id EQ 2023 ; time GE -15 Days) 
     */

    function setSelect($val) {
        $this->select = $val;
    }

    function setIndexName($val) {
        $this->indexName = $val;
    }

    function query($query, $limit = 0) {
        $op = array("EQ", "NE", "IN", "LE", "LT", "GE", "GT", "BETWEEN", "NOT_NULL", "NULL", "CONTAINS", "NOT_CONTAINS", "BEGINS_WITH");
        $regex = "/\b(EQ|NE|IN|LE|LT|GE|GT|BETWEEN|NOT_NULL|NULL|CONTAINS|NOT_CONTAINS|BEGINS_WITH)\b/i";

        $array = explode(":", $query);
        $tableName = trim($array[0]);
        //$query = explode(",", $array[1]);
        $query = $array[1];
        if (preg_match('/\b(AND|OR)\b/i', $query[0])) {
            $res = $this->selectCustom($tableName, $query);
            return $res;
        }
        if (isset($this->select))
            $select = $this->select;
        if (isset($this->indexName))
            $indexName = $this->indexName;
//        if (sizeof($query) == 2) {
//            if (preg_match('/Select/', $query[1])) {
//                $temp = explode("=>", $query[1]);
//                $select = $temp[1];
//            }
//            if (preg_match('/IndexName/', $query[1])) {
//                $temp = explode("=>", $query[1]);
//                $indexName = $temp[1];
//            }
//        }
//        if (sizeof($query) == 3) {
//            if (preg_match('/Select/', $query[1])) {
//                $temp = explode("=>", $query[1]);
//                $select = $temp[1];
//                $temp = explode("=>", $query[2]);
//                $indexName = $temp[1];
//            }
//            if (preg_match('/Select/', $query[2])) {
//                $temp = explode("=>", $query[1]);
//                $indexName = $temp[1];
//                $temp = explode("=>", $query[2]);
//                $select = $temp[1];
//            }
//        }


        $query = explode(";", $query);
        $queryJson = (object) null;


        $queryArray = "{\"Query\":[";
        for ($i = 0; $i < sizeof($query); $i++) {
            $q = preg_split($regex, $query[$i]);
            $operator = preg_match($regex, $query[$i], $matches);
            $queryJson->Key = trim($q[0]);
            $queryJson->Operator = trim($matches[0]);
            if (trim($matches[0]) == 'BETWEEN' || trim($matches[0]) == 'IN') {
                $fAtt = array();
                $q[1] = str_replace("[", "", $q[1]);
                $q[1] = str_replace("]", "", $q[1]);
                $many = explode(",", $q[1]);
                if (sizeof($many) > 0) {
                    for ($attI = 0; $attI < sizeof($many); $attI++) {
                        array_push($fAtt, trim($many[$attI]));
                    }
                    $queryJson->Value = $fAtt;
                }
                else
                    throw new Exception("Insufficient Values given");
            }
            else
                $queryJson->Value = trim($q[1]);
            if ($i < sizeof($query) - 1) {
                $queryArray .= json_encode($queryJson) . ",";
            } else {
                $queryArray .= json_encode($queryJson);
            }
        }
        $queryJson = (object) null;

        if (isset($select)) {
            $queryJson->Select = trim($select);
        }
        if (isset($indexName)) {
            $queryJson->IndexName = trim($indexName);
        }
        if (isset($select) || isset($indexName)) {
            $queryArray .= "],";
        }
        else
            $queryArray .= "]";
        if ($queryJson != null) {
            $str = str_replace("{", "", json_encode($queryJson));
            $str = str_replace("}", "", $str);
            //$str = str_replace("", "", $str);
            $queryArray .= $str . "}";
        }

        
        
        $this->clear();
        return $this->dynamodbClass->query($tableName, $queryArray, $limit);
    }

    function multiexplode($delimiters, $string) {
        $ary = explode($delimiters[0], $string);
        array_shift($delimiters);
        if ($delimiters != NULL) {
            foreach ($ary as $key => $val) {
                $ary[$key] = $this->multiexplode($delimiters, $val);
            }
        }
        return $ary;
    }

    /*
     * Delete Json : 
     * delete(tableName : Key => Value ; Key => Value)
     */

    function delete($query) {
        $temp = explode(":", $query);
        $tableName = trim($temp[0]);
        $AttributesJson = (object) null;
        $Attributes = "{\"Delete\":[";
        $temp = explode(";", $temp[1]);
        for ($i = 0; $i < sizeof($temp); $i++) {
            $attr = explode("=>", $temp[$i]);
            $AttributesJson->Key = trim($attr[0]);
            $AttributesJson->Value = trim($attr[1]);
            if ($i < sizeof($temp) - 1) {
                $Attributes .= json_encode($AttributesJson) . ",";
            } else {
                $Attributes .= json_encode($AttributesJson);
            }
        }
        $Attributes .= "]}";
        $this->dynamodbClass->deleteItem($tableName, $Attributes);
        $this->clear();
    }

    /*
     * query ( TableName : (id>1201 and time>-15 days) or (time > -10 days))
     * Ex: selectCustom(tableName: );
     */

    function selectCustom($tableName, $query) {
        $parser = new PHPSQLParser($query[0]);

        $parsed = $parser->parsed;

        $all = array();

        $i = 1;
        foreach ($parsed['WHERE'] as $key) {
            $node = $this->getNode('base_expr', $key);


            $all[$i] = array();
            foreach ($node as $key => $val) {
                array_push($all[$i], $val);
            }
            $i++;
        }
        $operator;
        $iterator = array();
        $i = 0;
        foreach ($all as $key => $val) {

            if ($this->array_depth($val) > 1) {
                $queryString = $tableName . ": ";
                foreach ($val as $keyss => $vals) {
                    $queryString .= trim($vals[0], "()") . " ";
                }
                $query = str_replace("and", ";", $queryString);
                $iterator[$i++] = $this->query($query);
            } else {
                $queryS = $tableName . ": ";
                if (!preg_match('/\b(AND|OR)\b/i', $val[0])) {
                    $queryS .= trim($val[0], "()") . " ";

                    $query = str_replace("and", ";", $queryS);

                    $iterator[$i++] = $this->query($query);
                } else {
                    $operator = $val[0];
                }
            }
        }

        if (isset($operator)) {
            if ($operator == 'and') {

                if ((isset($iterator[0]) && $iterator[0]['Count'] > 0) && (isset($iterator[1]) && $iterator[1]['Count'] > 0)) {
                    return array_merge($iterator[0]->get('Items'), $iterator[1]->get('Items'));
                }
                else
                    throw new Exception('No record found');
            }
            elseif ($operator == 'or') {
                if (isset($iterator[0]) && $iterator[0]['Count'] > 0) {
                    return $iterator[0]->get('Items');
                } elseif (isset($iterator[1]) && $iterator[1]['Count'] > 0) {
                    return $iterator[1]->get('Items');
                } else {
                    throw new Exception('No record found');
                }
            } else {
                throw new Exception('No record found');
            }
        }
        $this->clear();
    }

    function getNode($needle, $target) {

        $res = array();
        foreach ($target as $key => $val) {
            if ($key === $needle && ( (is_array($target['sub_tree']) && $this->array_depth($target['sub_tree']) < 3) || is_bool($target['sub_tree']) )) {
                array_push($res, $target[$key]);
            }
            if ($key === 'sub_tree' && !is_bool($key)) {
                if (is_array($val) && $this->array_depth($val) > 2)
                    foreach ($val as $value)
                        array_push($res, $this->getNode($needle, $value));
            }
        }
        return $res;
    }

    function array_depth(array $array) {
        $max_depth = 1;

        foreach ($array as $value) {
            if (is_array($value)) {
                $depth = $this->array_depth($value) + 1;

                if ($depth > $max_depth) {
                    $max_depth = $depth;
                }
            }
        }

        return $max_depth;
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
     * 
     * update('tableName','key => value ; key => value'//Keys,'key => Value => Action ; key => Value => Action'//UpdateAttrs)
     * ex : update('thread','ForumName => AmazonDB ; Subject => Number','LastPostedBy => asd@asd.com => PUT');
     */

    function update($tableName, $keys, $UpdateAttributes) {
        $updateJsonArray = "{\"Attributes\":[";
        $updateJson = (object) null;
        $keys = explode(';', $keys);
        for ($i = 0; $i < sizeof($keys); $i++) {
            $temp = explode("=>", $keys[$i]);
            $updateJson->Key = trim($temp[0]);
            $updateJson->Value = trim($temp[1]);
            if ($i < sizeof($keys) - 1) {
                $updateJsonArray .= json_encode($updateJson) . ",";
            } else {
                $updateJsonArray .= json_encode($updateJson);
            }
        }
        $updateJsonArray .= "],\"UpdateAttributes\":[";

        $keys = explode(";", $UpdateAttributes);
        for ($i = 0; $i < sizeof($keys); $i++) {
            $temp = explode("=>", $keys[$i]);
            $updateJson->Key = trim($temp[0]);
            $updateJson->Value = trim($temp[1]);
            $updateJson->Action = trim($temp[2]);
            if ($i < sizeof($keys) - 1) {
                $updateJsonArray .= json_encode($updateJson) . ",";
            } else {
                $updateJsonArray .= json_encode($updateJson);
            }
        }
        $updateJsonArray .= "]}";
        $this->clear();
        return $this->dynamodbClass->updateItem(trim($tableName), $updateJsonArray);
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
    function scanD($tableName,$query){
        $queryJson = (object)null;
        $queryArray = "{\"ScanFilter\":[";
        $query = trim($query,"()");
        echo "<br/>";
        $ex = explode("and",$query);
        if(sizeof($ex)>1){
            for($i=0;$i<sizeof($ex);$i++){
                $ex[$i] = str_replace("(","",$ex[$i]);
                $ex[$i] = str_replace(")","",$ex[$i]);
                
                $st = explode(" ", trim($ex[$i]));
                $queryJson->Key = trim($st[0]);
                $queryJson->Operator = trim($st[1]);
                $queryJson->Value = trim($st[2]);
                if($i<sizeof($ex)-1)
                    $queryArray .= json_encode($queryJson).",";
                else
                    $queryArray .= json_encode($queryJson);
            }
        }
        else{
            $st = explode(" ",trim($query));
            $queryJson->Key = trim($st[0]);
            $queryJson->Operator = trim($st[1]);
            $queryJson->Value = trim($st[2]);
            $queryArray .= json_encode($queryJson);
        }
        $queryArray .= "]";
        if(isset($this->select)){
            $queryJson = (object)null;
            $queryJson->Select = $this->select;
            $queryArray .= ",".substr(json_encode($queryJson), 1, -1);
        }
        $queryArray .= "}";
        
        return $this->dynamodbClass->scan($tableName, $queryArray);
    }
    /*
     *  Set IndexName to query on a secondary index.
     *  Select: ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT .
     *  
     *  Value : Contains exactly one value, except for a BETWEEN or IN comparison, in which case it contains two values.
     *  Op :  EQ, NE, IN, LE, LT, GE, GT, BETWEEN, NOT_NULL, NULL, CONTAINS, NOT_CONTAINS, BEGINS_WITH. 
     *  query ( TableName : key Op Value ; Key Op Value)
     *  Ex. query( errors : id EQ 2023 ; time GE -15 Days) 
     */

    function scan($query, $limit = 0) {
        $op = array("EQ", "NE", "IN", "LE", "LT", "GE", "GT", "BETWEEN", "NOT_NULL", "NULL", "CONTAINS", "NOT_CONTAINS", "BEGINS_WITH");
        $regex = "/\b(EQ|NE|IN|LE|LT|GE|GT|BETWEEN|NOT_NULL|NULL|CONTAINS|NOT_CONTAINS|BEGINS_WITH)\b/i";

        $array = explode(":", $query);
        $tableName = trim($array[0]);
        //$query = explode(",", $array[1]);
        $query = $array[1];
        
        $parser = new PHPSQLParser($query);

        $parsed = $parser->parsed;
        
        $all = array();

        $i = 1;
        
        foreach ($parsed['WHERE'] as $key) {
            $node = $this->getNode('base_expr', $key);


            $all[$i] = array();
            foreach ($node as $key => $val) {
                array_push($all[$i], $val);
            }
            $i++;
        }
     //   print_r($all);
        $this->result = array();
        $this->scanEvaluate($tableName,$all);
        $this->condition = "return ";
        
        if($this->array_depth($this->result[0]) >1 )
        $this->result[0] = call_user_func_array('array_merge', $this->result[0]);
        if($this->array_depth($this->result[2]) >1 )
        $this->result[2] = call_user_func_array('array_merge', $this->result[2]);
        //print_r($this->result);
        $this->scanFinish($this->result);
        $this->condition .=" ;";
        //echo $this->condition;
        if(eval($this->condition)){
            $res = $this->returnScanResult($this->result);
           // print_r($res);
            $res = call_user_func_array('array_merge', $res);
            return $res;
        }
        else{
            throw new Exception("No Record Found");
        }
        $this->clear();
    }
    
    function returnScanResult(&$array){
        //print_r($array);
        $result = array();
        if($this->array_depth($array[0])>4){
            if($array[0][1]['Operator'] == 'or'){
                if($array[0][0]['isValid']){
                    
                    array_push($result,array(
                        'isValid' => $array[0][0]['isValid'],
                        'Result' => $array[0][0]['Result']
                    ));
               }
            else
                array_push($result, array(
                        'isValid' => $array[0][2]['isValid'],
                        'Result' => $array[0][2]['Result']
                    ));
            }
            if($array[0][1]['Operator'] == 'and'){
                //$res = array();
                array_push($result, $array[0][0]['Result']);
                array_push($result, $array[0][2]['Result']);    
            }
            $this->result[0] = $result;
            $this->result[0] = call_user_func_array('array_merge', $this->result[0]);
        }
        $result = array();
        
        
        if($this->array_depth($array[2])>4){
            if($array[2][1]['Operator'] == 'or'){
                if($array[2][0]['isValid']){
                    array_push($result,array(
                        'isValid' => $array[2][0]['isValid'],
                        'Result' => $array[2][0]['Result']
                    ));
            }
            else
                array_push($result, array(
                        'isValid' => $array[2][2]['isValid'],
                        'Result' => $array[2][2]['Result']
                    ));
            }
            if($array[1]['Operator'] == 'and'){
                //$res = array();
                array_push($result, $array[2][0]['Result']);
                array_push($result, $array[2][2]['Result']);    
            }
           $this->result[2]= $result;
           $this->result[2] = call_user_func_array('array_merge', $this->result[2]);
        }
        if($array[1]['Operator'] == 'or'){
            if($array[0]['isValid']){
                return $array[0]['Result'];
            }
            else
                return $array[2]['Result'];
        }
        if($array[1]['Operator'] == 'and'){
            $res = array();
            array_push($res,$array[0]['Result']);
            array_push($res,$array[2]['Result']);
            return $res;
        }
        
    }
    
    function scanFinish($array){
        
        foreach($array as $key){
            if(($this->array_depth($key))>5){
                $this->condition .= " ( ".$key[0]['isValid']." ".$key[1]['Operator']." ".$key[2]['isValid']." ) ";
            }
            elseif(isset($key['isValid'])){
                $this->condition .= " ( ".$key['isValid']." ) ";
            }
            elseif(isset($key['Operator']))
                $this->condition .= " ".$key['Operator']." ";
        }
    }
    
    function scanEvaluate($tableName,&$array){
        foreach($array as $key){
            
            if(($this->array_depth($key)>2)){
                $this->scanEvaluate($tableName,$key);   
             }
            if(sizeof($key) > 1){
                $query = "";
                if($key[1][0] == 'or'){
                    
                    $part = array();
                    $query = $key[0][0];
                    $result = $this->scanD($tableName,$query);
                    
                    $result = call_user_func_array('array_merge', $result);
                    
                    array_push($part,$result);
                    array_push($part,array('Operator' => $key[1][0]));
                    $query = $key[2][0];
                    $result = $this->scanD($tableName,$query);
                    $result = call_user_func_array('array_merge', $result);
                    
                    $res = array(
                        'isValid' => 1,
                        'Result' => array('x' => 's')
                    );
                    array_push($part,$result);
                    array_push($this->result,$part);
                }
                elseif($key[1][0] == 'and'){
                    $query .= "( ".$key[0][0]."  and  ".$key[2][0]."  )";
                   $result =  $this->scanD($tableName,$query);
                   //print_r($result);
                   //$result = call_user_func_array('array_merge', $result);s
                    array_push($this->result,$result);
                }
                
            }
            else{
                if(!preg_match('/\b(AND|OR)\b/i', $key[0])){
                    $query = $key[0];
                    $result = $this->scanD($tableName,$query);
                    array_push($this->result,$result);
                }
                else{
                    array_push($this->result,array(
                            'Operator' => $key[0]
                        )
                    );
                }
            }
        }
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

$AWS_SECRET = 'zblB2e3L+6N1SMV16CCD0D5sVxsDa0XYcFZyJOIZ';
$AWS_KEY = 'AKIAJZL24KVRAOAXFEFQ';
$AWS_REGION = 'us-west-2';
$dynamodb = new dynamoDBInput($AWS_KEY, $AWS_SECRET, $AWS_REGION);
//$res = $dynamodb->query( 'errors : where ( ( id EQ 1201 ) and ( time GT -15 Days ) ) and ( id EQ 2003 ) ');
//print_r($dynamodb->orderBy($res, 'id','asc'));
//$dynamodb = new dynamoDBInput();
//$dynamodb ->setUnits(10, 5);
//$dynamodb -> createTable('Errors, PrimaryIndex : CustomerID as N of HASH;OrderID as N of RANGE','LocalSecondaryIndex : { CustomerID as N of HASH;OrderDate as N of RANGE | OrderDateIndexs | INCLUDE | [ CustomerID , OrderID ]}, { CustomerID as N of HASH;OrderDate as N of RANGE | OrderDateIndexes | KEYS_ONLY } ');
//$dynamodb->insert('insert into Orders : CustomerID => 3 ; OrderID => 3 ; OrderDate => 31/06/2013 ; Remarks => Good');
//$res = $dynamodb->query( 'Orders : id EQ 1 ; OrderDate BETWEEN [ 31/06/2013 , 30/07/2013 ]');
//$res = $dynamodb->query('Orders : CustomerID EQ 1 ; OrderID BETWEEN [ 31/06/2013 , 30/07/2013 ]');


//Select by limit. For single row set $limit to 1
//$dynamodb->query( 'errors : id EQ 1201 ; time GT -15 Days',$limit);
//
        //$dynamodb->delete('errors: id => 2023 ; time => 1372614625');
$dynamodb->update('Orders',' CustomerID => 2 ; OrderID => 2 ','Email => asd@asd.com => PUT');
//$dynamodb->setSelect('COUNT');
//$res = $dynamodb->scan( 'Orders : where ( CustomerID EQ 2 )  and ( ( CustomerID EQ 3 ) and ( OrderID EQ 3) )');
//$dynamodb->scanD('Orders','( CustomerID EQ 2 )');
//print_r($dynamodb->orderBy($res, 'CustomerID','desc'));
//$dynamodb->delete(' Orders : CustomerID => 1 ; OrderID => 1 ');


?>
