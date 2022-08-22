<?php

namespace app\Components\RetailCrm;

use App\Components\Bigquery\BigqueryIntegration;
use App\Components\RetailCrm\RetailCrmIntegration;

class RetailCrmHistoryStatuses
{
    private $retailcrm;
    private $token;
    private $uri;
    private array $headers;
    private $schemaBigqueryTable;

    public function __construct()
    {
        $this->retailcrm = new RetailCrmIntegration;
        $this->token = env("RETAILCRM_TOKEN");
        $this->uri = "https://rtl2.retailcrm.ru/api/v5/";
        $this->headers = [
            "headers" => [
                "X-API-KEY" => $this->token
            ]
        ];
        $this->schemaBigqueryTable = [
            'fields' => [ 
                ['name' => 'order_id', 'type' => 'STRING'],

                ['name' => 'new', 'type' => 'STRING'],
                ['name' => 'new_created_at', 'type' => 'DATETIME'],

                ['name' => 'assembling', 'type' => 'STRING'],
                ['name' => 'assembling_created_at', 'type' => 'DATETIME'],

                ['name' => 'assembling_complete', 'type' => 'STRING'],
                ['name' => 'assembling_complete_manager_name', 'type' => 'STRING'],
                ['name' => 'assembling_complete_created_at', 'type' => 'DATETIME'],

                ['name' => 'delivery', 'type' => 'STRING'],
                ['name' => 'delivery_created_at', 'type' => 'DATETIME'],

                ['name' => 'completed', 'type' => 'STRING'],
                ['name' => 'completed_created_at', 'type' => 'DATETIME'],

                ['name' => 'canceled', 'type' => 'STRING'],
                ['name' => 'canceled_created_at', 'type' => 'DATETIME'],
            ]
        ];
    }

    /**
     * Необходимо чуть было изменить статусы и категории статусов для таблицы history_statuses
     * @return array
     */
    private function getStatusesCategory()
    {
        $statuses = $this->retailcrm->getStatusesCategory();
        unset($statuses["soglasovanie"]);
        unset($statuses["soglasovanienasklade"]);
        unset($statuses["trade"]);
        unset($statuses["komplektaciya"]);
        $statuses["assembling"] = ["assembling"];
        $statuses["assembling_complete"] = ["assembling-complete"];

        return $statuses;
    } 

    public function getSchema()
    {
        return $this->schemaBigqueryTable;
    }

    /**
     * Создание обьекта BigqueryIntegration
     */
    private function initBigqueryClient(string $projectId)
    {
        return new BigqueryIntegration($projectId);
    } 

    /**
     * Создает новую таблицу $tableName в проекте $projectId, в датасете $datasetId
     * @return имя созданной таблицы 
     */
    public function createTableBq(string $projectId, string $datasetId, string $tableName, array $schema)
    { 
        return $this->initBigqueryClient($projectId)->createTable($datasetId, $tableName, $this->schemaBigqueryTable);
    }

    /**
     * Собирает историю изменений заказов от $startDate и до $endDate
     * @param string date format Y-m-d H:i:s
     * @return array [$orderId, $managerId, $switchTime, $oldStatus, $newStatus] 
     */
    public function getHistory(string $startDate, string $endDate): array
    {
        $totalPageCount = $this->retailcrm->totalPageCount("orders/history", $this->uri, $this->headers, "filter[startDate]=$startDate&filter[endDate]=$endDate");
        
        $objs = [];
        // пагинация
        for ($i = 1; $i < $totalPageCount; ++$i)
        {
            $responce = $this->retailcrm->request("get", $this->uri . "orders/history?page=$i&filter[startDate]=$startDate&filter[endDate]=$endDate", $this->headers);

            // просмотреть каждое изменение
            foreach($responce["history"] as $part)
            {
                $field = $part["field"];
                
                //отбор только изменений, связанных со статусом 
                if ($field !== "status")
                    continue;

                $objs[] = [
                    "orderId" => $part["order"]["id"] ?? null, 
                    "managerId" => $part["user"]["id"] ?? null,
                    "switchTime" => $part["createdAt"] ?? null,
                    "newStatus" => $part["newValue"]["code"] ?? null,
                ];
            }
        }
        
        return $objs;
    }

    /**
     * По подстатусу $status (недозвон, курьер просрочка и тд) определить категорию (новый, согласование и тд)
     * @return status
     */
    private function getStatusCategory($status)
    {
        $statuses = $this->getStatusesCategory();
        foreach (array_keys($statuses) as $category)
        {
            foreach ($statuses[$category] as $var)
            {
                if ($var === $status)
                    return $category;
            }
        }
        return null;
    }
    
    private function getSchemaTableHistoryStatuses()
    {
        $schema = [];
        foreach ($this->schemaBigqueryTable["fields"] as $field)
            $schema[$field["name"]] = null;

        return $schema;
    }

    /**
     * Обновить строку $data в Bigquery в таблице $tableId,
     * в датасете $datasetId, в проекте $projectId по ключу $orderId
     */
    public function createUpdateQuery(string $projectId, string $datasetId, string $tableId, array $data, string $orderId)
    {
        $bqClient = $this->initBigqueryClient($projectId);

        $cols_values = "";
        foreach(array_keys($data) as $key)
        {
            $tmp = $data[$key] === null ? 'null' : $data[$key];
            if ($tmp === 'null')
            {
                $value = "$tmp";
            }                 
            elseif (is_a($tmp, 'DateTime'))
            {
                $tmp = $tmp->format("Y-m-d H:i:s");
                $value = "'$tmp'";
            }
            else
            {
                $value = "'$tmp'";
            }
            $cols_values .= "`$key`" . ' = ' . $value . ", ";
        }            

        $cols_values = substr_replace($cols_values, "" , -2);
        $query = sprintf("UPDATE `%s` SET $cols_values WHERE order_id = '%s';", "$projectId.$datasetId.$tableId", $orderId);
        try
        {
            return $bqClient->query($query); 
        }
        catch (\Exception $e)
        {
            return 0;
        }      
    }

    /**
     * Если заявка не была замечена в истории изменений $this->getHistory, то добавить ее в массив
     * @param $data - ссылка
     */
    private function setNewStatuses(string $status, array &$data, array $row, array $users)
    {
        $data["order_id"] = $row["orderId"];
        $data[$status] = "TRUE";
        if ($status === "assembling_complete")
            $data["assembling_complete_manager_name"] = $this->retailcrm->getManagerName($users, $row["managerId"]);
            
        $data[$status . "_created_at"] = $row["switchTime"];
    }

    /**
     * Если заявка существует в истории изменений $this->getHistory, то добавить в нее обновленный статус
     * @param $data - ссылка
     */
    private function setUpdateStatuses($status, array &$data, array &$row, array $users)
    {
        if ($status === null)
            return 0;

        $data[$status] = "TRUE"; 
        
        if ($status === "assembling_complete")
            $data["assembling_complete_manager_name"] = $this->retailcrm->getManagerName($users, $row["managerId"]);

        $data[$status ."_created_at"] = $row["switchTime"];

        return 1;
    }

    /**
     * Объединить историю изменений в 1 массив
     * @param $rows - parts of changes 
     * @return array
     */
    public function concatChanges(array $rows): array
    {
        $objs = [];
        // get all users to identify managers names
        $users = $this->retailcrm->getUsers($this->uri, $this->headers);

        foreach($rows as $row)
        {
            $flag = false;

            foreach($objs as &$obj)
            {
                if ($row["orderId"] != $obj["order_id"])
                    continue;
        
                $categoryNewStatus = $this->getStatusCategory($row["newStatus"]);
                $this->setUpdateStatuses($categoryNewStatus, $obj, $row, $users);
                $flag = true;
                break;
            }

            if ($flag)
                continue;

            $data = $this->getSchemaTableHistoryStatuses();
            $categoryNewStatus = $this->getStatusCategory($row["newStatus"]);
            
            if ($categoryNewStatus === null)
                continue;

            $this->setNewStatuses($categoryNewStatus, $data, $row, $users);

            $objs[] = $data;
        }

        return $objs;
    }

    
    /**
     * Устаналивает нужную букву в конец идентификатора заказа 
     */
    public function pack()
    {
        return $this->retailcrm->request("get", $this->uri . "orders/63141?by=id", $this->headers);
    }

    /**
     * Устаналивает нужную букву в конец идентификатора заказа 
     */
    public function setCorrectOrderId(array &$rows)
    {
        foreach($rows as &$row)
        {
            $id = $row['order_id'];
            $newCreatedAt = $row['new_created_at'];
            try 
            {
                $res = $this->retailcrm->request("get", $this->uri . "orders/$id?by=id", $this->headers);
            }
            catch (\Exception $e)
            {
                continue;
            }
            

            if ($res["success"] !== true)
                continue;

            $row["order_id"] = $res["order"]["id"] === $id ? $res["order"]["number"] : $row["order_id"];
        }
        
        return 1;
    }

    /**
     * загружает исторические данные по статусам в bq из crm
     * @param array $rows - массив изменений статусов
     * 
     */
    public function loadHistoryBq(string $projectId, string $datasetId, string $tableId, array $rows)
    {
        $bqClient = $this->initBigqueryClient($projectId);

        $columns = array_keys($this->getSchemaTableHistoryStatuses());

        foreach($rows as &$row)
        {
            $order_id = $row["order_id"];
        
            $flag = false;

            $query = sprintf("SELECT * FROM `%s.%s.%s` WHERE order_id = '%s';", $projectId, $datasetId, $tableId, $order_id);
            $res = $bqClient->query($query)->rows();
            foreach($res as $obj)
            {   
                foreach($columns as $column)
                {   
                    $bqData = $obj[$column];
                    $data = $row[$column];

                    if ($bqData !== null and $data === null)
                        $row[$column] = $bqData; 
                }
                
                $this->createUpdateQuery($projectId, $datasetId, $tableId, $row, $order_id);

                $flag = true;
            }

            $row["new"] = "TRUE";
            if ($row["new_created_at"] === null) {
                $val = $this->getNums((string)$row["order_id"]);
                $res = $this->retailcrm->request(
                    "get", 
                    $this->uri . "orders/history?page=1&filter[orderId]=$val", 
                    $this->headers
                );
                $createdAt = $res["history"][0]["createdAt"] ?? null;
                $row["new_created_at"] = $createdAt;
            }
            if ($flag === false)
                $bqClient->insertRow($datasetId, $tableId, $row);
        }

        return 1;        
    }

    public function getNums(string $str)
    {   
        $new_str = "";
        foreach (str_split($str) as $sym) {
            if (is_numeric($sym)) {
                $new_str .= $sym;
            }
        }
        
        return $new_str;
    }

    public function test()
    {
        return $this->retailcrm->request("get", $this->uri . "users/30", $this->headers);
    }
}