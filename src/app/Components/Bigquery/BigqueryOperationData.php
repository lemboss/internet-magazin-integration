<?php

namespace app\Components\Bigquery;

use App\Components\Bigquery\BigqueryIntegration;

class BigqueryOperationData extends BigqueryIntegration
{
    private string $datasetId;
    private string $tableId;

    public function __construct(string $projectId, string $datasetId, string $tableId)
    {
        parent::__construct($projectId);
        $this->datasetId = $datasetId;
        $this->tableId = $tableId;
    }

    public function getSchema()
    {
        return [
            'fields' => [ 
                ['name' => 'date', 'type' => 'DATETIME'],
                ['name' => 'account', 'type' => 'STRING'],
                ['name' => 'contragent', 'type' => 'STRING'],
                ['name' => 'type', 'type' => 'STRING'],
                ['name' => 'article', 'type' => 'STRING'],
                ['name' => 'sub_aricle_first', 'type' => 'STRING'],
                ['name' => 'sub_aricle_second', 'type' => 'STRING'],
                ['name' => 'sub_aricle_third', 'type' => 'STRING'],
                ['name' => 'comment', 'type' => 'STRING'],
                ['name' => 'project', 'type' => 'STRING'],
                ['name' => 'value', 'type' => 'FLOAT'],
                ['name' => 'remainder', 'type' => 'FLOAT'],
                ['name' => 'company', 'type' => 'STRING'],
                ['name' => 'company_remainder', 'type' => 'FLOAT'],
                ['name' => 'is_net_income', 'type' => 'BOOL']
            ]
        ];
    }

    public function createEmptyTable()
    {
        $schema = $this->getSchema();
        return parent::createTable($this->datasetId, $this->tableId, $schema);
    }

    public function insertData(array $data)
    {
        return parent::insertRows($this->datasetId, $this->tableId, $data);
    }
    
    public function getDataBq(string $date_start, string $date_end)
    {
        $dataBq = parent::getTable($this->datasetId, $this->tableId)->rows();
        $rows = [];
        foreach ($dataBq as $row) {
            $date = parent::convertDateToString($row["date"], "Y-m-d");
            if ($date >= $date_start and $date <= $date_end) {
                $row["date"] = $date;
                $rows[] = $row;
            } 
        }

        return $rows;
    }

    public function clearByDateStart(string $filterDate)
    {
        $query = sprintf(
            "DELETE FROM `%s.%s.%s` WHERE date >= '%s'",
            $this->projectId,
            $this->datasetId,
            $this->tableId,
            $filterDate
        );
        return parent::query($query);
    }
}