<?php

namespace app\Components\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\InsertResponse;

class BigqueryIntegration
{
    protected BigQueryClient $client;
    protected string $projectId;

    public function __construct(string $projectId)
    {
        $keys = dirname(__DIR__, 3) . '/bg-keys.json';
        putenv("GOOGLE_APPLICATION_CREDENTIALS={$keys}");

        $this->projectId = $projectId;
        $this->client = new BigQueryClient([
            'projectId' => $projectId,
        ]);
    }

    /**
     * insert rows to bigquery table
     * @param array $row - row to write to bigquery
     * @param Google\Cloud\BigQuery\Table $table
     * @return Google\Cloud\BigQuery\InsertResponse - return data
     */
    public function insertRows(string $datasetId, string $tableId, array $data)
    {
        if ($data === []) {
            return null;
        }
        
        $dataset = $this->client->dataset($datasetId);
        $table = $dataset->table($tableId);

        foreach($data as $row)
        {
            $rows[] = [
                'data' => $row
            ];
        }

        return $table->insertRows($rows);
    }

    /**
     * insert row to bigquery table
     * @param array $row - row to write to bigquery
     * @param Google\Cloud\BigQuery\Table $table
     * @return Google\Cloud\BigQuery\InsertResponse - return data
     */
    public function insertRow(string $datasetId, string $tableId, array $row)
    {
        $dataset = $this->client->dataset($datasetId);
        $table = $dataset->table($tableId);
        $rows = [
            [
                'data' => $row
            ]
        ];
        
        return $table->insertRows($rows);
    }

    /**
     * insert rows to bigquery table
     * if count($row) == 1: call func like arg $row = [$row]
     * else: call func like $row
     * @param $row - row to write to bigquery
     * @param Google\Cloud\BigQuery\Table $table
     * @return Google\Cloud\BigQuery\InsertResponse - return data
     */
    public function insertRowsDto($row, $table)
    {
        if ($row === [])
            return;
        $rows = [];
        foreach ($row as $obj)
        {
            $rows[] = [
                'data' => $obj->toArray() ?? ""
            ];
        }

        return $table->insertRows($rows);
    }

    /**
     * send data to table
     * @param Spatie\DataTransferObject\DataTransferObject $row - row to write to data table
     * @param string $datasetId 
     * @param string $tableId
     * @return Google\Cloud\BigQuery\InsertResponse - return data
     */
    public function sendDataDto($row, $datasetId, $tableId): InsertResponse
    {
        $dataset = $this->client->dataset($datasetId);
        $table = $dataset->table($tableId);
        $resData = $this->insertRowsDto($row, $table);

        return $resData;
    }

    public function getDatasets(): array
    {
        $datasets = $this->client->datasets();
        $ids = [];
        foreach ($datasets as $dataset) 
            $ids[] = $dataset->id();
        

        return $ids;
    }

    public function getTables($datasetId): array
    {
        $tables = $this->client->dataset($datasetId);
        $ids = [];
        foreach ($tables as $table) 
            $ids[] = $table->id();

        return $ids;
    }

    /**
     * clear dataset
     * type "delete" for begin
     * @param string $code
     */
    public function clearTable(string $code, string $projectId, string $datasetId, string $tableId)
    {
        if ($code != "delete") 
            return;
        $query = sprintf("DELETE FROM `%s.%s.%s` WHERE true;", $projectId, $datasetId, $tableId);
        $queryJobConfig = $this->client->query($query);
        $this->client->runQuery($queryJobConfig);
    }

    /**
     * run select sql query
     * @param string $query example "SELECT * FROM `project.dataset.table` WHERE column = 'value';" 
     * @return Google\Cloud\BigQuery\QueryResults
     */
    public function query(string $query)
    {
        $queryJobConfig = $this->client->query($query);
        return $this->client->runQuery($queryJobConfig);
    }

    public function createTable(string $datasetId, string $tableName, array $schema)
    {
        $dataset = $this->client->dataset($datasetId);
        if (!$dataset->table($tableName)->exists()) 
            $dataset->createTable($tableName, ['schema' => $schema]);
        return $dataset->table($tableName)->id();
    }

    public function convertDateToString($data, string $format)
    {
        return $this->client->date($data)->get()->format($format);
    }

    public function getTable(string $datasetId, string $tableId)
    {
        $dataset = $this->client->dataset($datasetId);
        return $dataset->table($tableId);
    }
}
