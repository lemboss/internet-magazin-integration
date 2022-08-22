<?php

namespace app\Components\PlanFact;

use App\Components\PlanFact\PlanFactIntegration;

class PlanFactActualRemainder
{
    private $pf;
    private $schema;

    public function __construct()
    {
        $this->pf = new PlanFactIntegration;
        $this->schema = [
            'fields' => [ 
                ['name' => 'account', 'type' => 'STRING'],
                ['name' => 'account_remainder', 'type' => 'FLOAT'],
                ['name' => 'company', 'type' => 'STRING'],
                ['name' => 'company_remainder', 'type' => 'FLOAT']   
            ]
        ];
    }

    public function getSchema()
    {
        return $this->schema;
    }

    public function getData()
    {
        $changes = $this->pf->getAccountsHistory();
        $startingValues = $this->pf->concatAccountsStartingValues();
        $accountsInfo = $this->pf->getAccounts();
        $companiesAccounts = $this->pf->getCompaniesAccounts($accountsInfo); 
        $accountsBalance = $this->pf->getChangesBalance($changes, $startingValues);
        $this->pf->delSpacesBalance($accountsBalance);
        $companiesBalance = $this->pf->getCompaniesBalance($companiesAccounts, $accountsBalance);
        $date = date("Y-m-d", time());
        $objs = [];
        foreach (array_keys($companiesAccounts) as $company) {
            foreach ($companiesAccounts[$company] as $account) {
                if ($accountsBalance[$account][$date][0] === $accountsBalance[$account][$date][1]) {
                    $objs[] = [
                        "account" => $account,
                        "account_remainder" => $accountsBalance[$account][$date][0],
                        "account_remainder_another_currency" => null,
                        "company" => $company,
                        "company_remainder" => $companiesBalance[$company][$date][0],
                        "company_remainder_another_currency" => null
                    ];
                } else {
                    $objs[] = [
                        "account" => $account,
                        "account_remainder" => null,
                        "account_remainder_another_currency" => $accountsBalance[$account][$date][1],
                        "company" => $company,
                        "company_remainder" => null,
                        "company_remainder_another_currency" => $companiesBalance[$company][$date][1]
                    ];
                }
            }
        }

        return $objs;
    }

    /**
     * accounts which table dont contains 
     */
    public function getNewAccounts(array $accountsBq, array $accountsApi)
    {
        $newAccounts = [];
        
        foreach ($accountsApi as $account) {
            if (!in_array($account, $accountsBq)) {
                $newAccounts[] = $account; 
            }
        }

        return $newAccounts;
    }

    /**
     * find accounts to delete from table
     */
    public function getOldAccounts(array $accountsBq, array $accountsApi)
    {
        $delAccounts = [];

        foreach ($accountsBq as $account) {
            if (!in_array($account, $accountsApi)) {
                $delAccounts[] = $account;
            }
        }

        return $delAccounts;
    }

    public function getAccountsBq(array $rowsBq)
    {
        $accountBq = [];
        foreach ($rowsBq as $row) {
            $accountBq[] = $row["account"];
        }

        return $accountBq;
    }

    public function getAccountsApi(array $rowsApi)
    {
        $accountApi = [];
        foreach ($rowsApi as $row) {
            $accountApi[] = $row["account"];
        }

        return $accountApi;
    }

    public function getUpdatesAccount(array $accountsToAdd, array $accountsToDel, array $accountApi)
    {
        $accountsToUpdate = [];
        foreach ($accountApi as $account) {
            if (!in_array($account, $accountsToAdd) and !in_array($account, $accountsToDel)) {
                $accountsToUpdate[] = $account;
            }
        }

        return $accountsToUpdate;
    }

    public function getRowsToManipulate(array $account, array $rowsApi)
    {
        $rows = [];

        foreach ($rowsApi as $row) {
            if (in_array($row["account"], $account)) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    public function saveCsv(array $data, string $path)
    {
        // open csv file for writing
        $f = fopen($path, 'w');
        
        fputcsv($f, array_keys($data[0]));

        // write each row at a time to a file
        foreach ($data as $row) {
            fputcsv($f, array_values($row));
        }

        // close the file
        fclose($f);

        return 1;
    }
}