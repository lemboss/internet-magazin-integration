<?php

namespace app\Components\Bigquery;

use App\Components\Bigquery\BigqueryIntegration;

class BigqueryActualRemainder extends BigqueryIntegration
{
    private string $datasetId;
    private string $tableId;

    public function __construct(string $projectId, string $datasetId, string $tableId)
    {
        parent::__construct($projectId);
        $this->datasetId = $datasetId;
        $this->tableId = $tableId;
    }
    
    public function queryDelRows(array $accountsToDel)
    {
        $accounts = implode("','", $accountsToDel);
        $query = sprintf(
            "DELETE FROM `%s.%s.%s` WHERE account = '$accounts';", 
            $this->projectId,
            $this->datasetId,
            $this->tableId
        );
        $res = parent::query($query);

        return $res;
    }

    public function queryAddRows(array $rowsToAdd)
    {
        $res = parent::insertRows($this->datasetId, $this->tableId, $rowsToAdd);
        return $res;
    }

    public function queryUpdateAccounts(array $rowsToUpdate)
    {
        $resQueries = [];
        $indexFailRow = 0;
        foreach ($rowsToUpdate as $row) {
            $newAccountRemainder = $row["account_remainder"];
            $newCompanyRemainder = $row["company_remainder"];
            $accountTitle = $row["account"];
            $query = sprintf(
                "UPDATE `%s.%s.%s` SET account_remainder = %s, company_remainder = %s WHERE account = '%s';", 
                $this->projectId,
                $this->datasetId,
                $this->tableId,
                $newAccountRemainder,
                $newCompanyRemainder,
                $accountTitle
            );
            try
            {
                parent::query($query);
            }
            catch (\Exception $e)
            {
                $resQueries[] = [$indexFailRow, $row];
            } 
            ++$indexFailRow;
        }

        return $resQueries;
    }

    public function getData()
    {
        $dataBq = parent::getTable($this->datasetId, $this->tableId)->rows();
        $rows = [];
        foreach ($dataBq as $row) {
            $rows[] = $row;
        }

        return $rows;
    }

    public function getSchema()
    {
        return [
            'fields' => [ 
                ['name' => 'account', 'type' => 'STRING'],
                ['name' => 'account_remainder', 'type' => 'FLOAT'],
                ['name' => 'account_remainder_another_currency', 'type' => 'FLOAT'],
                ['name' => 'company', 'type' => 'STRING'],
                ['name' => 'company_remainder', 'type' => 'FLOAT'],
                ['name' => 'company_remainder_another_currency', 'type' => 'FLOAT']
            ]
        ];
    }
}