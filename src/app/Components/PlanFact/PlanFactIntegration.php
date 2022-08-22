<?php

namespace app\Components\PlanFact;

use GuzzleHttp\Client;


class PlanFactIntegration
{
    /**
     * https://apidoc.planfact.io/
     */

    private $token;
    private $options;
    private $uri; 
    private $client;

    public function __construct()
    {
        $this->token = env("PLANFACT_TOKEN");
        $this->options = [
            "headers" => [
                'Accept' => 'application/json',
                "X-ApiKey" => $this->token
            ]
        ];
        $this->uri = "https://api.planfact.io/api/v1/";
        $this->client = new Client;
    }
    
    public function request(string $method, string $uri, array $options = [])
    {
        return ($this->client->request($method, $uri, $options))->getBody()->getContents();
    }

    /**
     * Убрать избыточную информацию по статьям
     * @param array &$articles - ссылка - [["Продажа товаров", "Доходы"], ["Продажа товаров", "Доходы"]]
     * @param array $checkedArticles - ["Доходы", "Расходы"];
     * @return array [["Продажа товаров"], ["Продажа товаров]]
     */
    public function unsetExcessArticle(array &$articles, array $checkedArticles)
    {
        foreach($articles as $_)
            $newArticles[] = [];

        $index = 0;
        foreach ($articles as $obj)
        {
            foreach ($obj as $subArticle)
            {
                if (in_array($subArticle, $checkedArticles))
                    continue;

                $newArticles[$index][] = $subArticle;
            }
            ++$index;
        }

        $articles = $newArticles;
    }

    /**
     * DOCS https://apidoc.planfact.io/#tag-Operations
     * get all info about operations
     * @param string $date_start - set starts date operations
     * @param string $date_end - set ends date operations
     * @return array responce 
     */
    public function getOperations(string $date_start, string $date_end): array
    {
        return json_decode($this->request("get", $this->uri . "operations?filter.operationDateStart=$date_start&filter.operationDateEnd=$date_end", $this->options), true);
    }

    public function getAccounts(): array
    {
        $responce = json_decode($this->request("get", $this->uri . "accounts", $this->options), true);
        
        if (!$responce["isSuccess"])
            return [];

        return $responce["data"];
        
    }

    /**
     * @param string $mode - id of returned array ("title", "accountId")
     */
    public function getAccountsStartingValues(array $responce): array
    {
        $data = [];
        foreach($responce["items"] as $item)
        {
            $date = $item["startingRemainderDate"] === null ? $item["createDate"] : $item["startingRemainderDate"];
            $date = date("Y-m-d", strtotime($date));
            $data[$item["accountId"]] = [
                "title" => $item["title"],
                "company" => $item["company"]["title"],
                "startingRemainderValueInUserCurrency" => $item["startingRemainderValueInUserCurrency"] ?? 0.0,
                "startingRemainderValue" => $item["startingRemainderValue"] ?? 0.0,
                "startingRemainderDate" => $date
            ];
        }

        return $data;
    }

    /**
     * get history of changes on accounts
     * @return array responce 
     */
    public function getAccountsHistory(): array
    {
        $responce = json_decode($this->request("get", $this->uri . "bizinfos/accountshistory", $this->options), true);
        
        if (!$responce["isSuccess"])
            return [];

        return $responce["data"];
    }

    /**
     * get history of changes on accounts
     * @return array responce 
     */
    public function getAccountInfo($accountId): array
    {
        $accountInfo = json_decode($this->request("get", $this->uri . "accounts/$accountId", $this->options), true);
        if (!$accountInfo["isSuccess"])
        {
            return [];
        }
        
        return $accountInfo["data"];
    }

    /**
     * get date from object
     * @param array &$data - object, 1d array
     * @return array foormat "Y-m-d"
     */
    public function getDate(array &$data): array
    {
        foreach($data["operationParts"] as $part)
            $dates[] = $part["calculationDate"];        
        
        return $dates;        
    }

    /**
     * get account title from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getAccountTitle(array &$data): array
    {
        return [$data["account"]["title"]];
    }

    /**
     * get operation type from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getOperationType(array &$data): array
    {
        switch ($data["operationType"])
        {
            case "Outcome":
                return ["Расходы"];
            case "Income":
                return ["Доходы"];
        }
        return [];
    }

    /**
     * get contr agent type from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getContrAgent(array &$data): array
    {
        foreach($data["operationParts"] as $part)
            $titles[] = $part["contrAgent"]["title"];
            
        return $titles;
    }

    /**
     * get operation category from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getCategory(array &$data): array
    {
        foreach($data["operationParts"] as $part) {
            $categoryIds[] = $part["operationCategory"]["operationCategoryId"];
        }
            
        $categories = [];
        $index = 0;
        foreach($categoryIds as $id)
        {
            $responce = json_decode($this->request("get", $this->uri . "operationcategories/$id", $this->options), true);
            $categories[$index][] = $responce["data"]["title"];
            $parentOperationCategoryId = $responce["data"]["parentOperationCategoryId"];
            while (true)
            {
                $responce = json_decode($this->request("get", $this->uri . "operationcategories/$parentOperationCategoryId", $this->options), true);
                
                $parentOperationCategoryId = $responce["data"]["parentOperationCategoryId"];
                $categories[$index][] = $responce["data"]["title"];
                
                if ($parentOperationCategoryId === null) {
                    break;
                }   
            }
            ++$index;   
        }

        return $categories;
    }

    /**
     * get comment from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getComment(array &$data): array
    {
        return [$data["comment"]];
    }

    /**
     * get value from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getValue(array &$data): array
    {
        foreach($data["operationParts"] as $part)
            $values[] = $part["value"];

        return $values;
    }

    /**
     * get project title from object
     * @param array &$data - object, 1d array
     * @return array 
     */
    public function getProject(array &$data): array
    {        
        foreach($data["operationParts"] as $part)
            $project[] = $part["project"]["title"];

        return $project;
    }

    /**
     * get all incomes and outcomes by accounts per day
     */
    public function getChangesBalance(array &$changes, array $startingValues): array
    {
        $data = [];

        foreach ($changes as $change)
        {
            $remainderRub = 0;
            $remainderUsd = 0;
            foreach($change["details"] as $oneDay)
            {
                $date = date("Y-m-d", strtotime($oneDay["date"]));
                $accountId = $change["accountId"];
                $title = $startingValues[$accountId]["title"];
                if (!isset($data[$title][$date]))
                {
                    $data[$title][$date][] = $startingValues[$accountId]["startingRemainderValueInUserCurrency"];
                    $data[$title][$date][] = $startingValues[$accountId]["startingRemainderValue"];

                }
                
                $remainderRub += $oneDay["factValueInUserCurrency"];
                $remainderUsd += $oneDay["factValue"];
                
                $data[$title][$date][0] += $remainderRub;
                $data[$title][$date][1] += $remainderUsd;
            } 
        }

        foreach ($startingValues as $value)
        {
            $title = $value["title"];
            if (!isset($data[$title], $data))
            {
                $data[$title] = [
                    $value["startingRemainderDate"] => [
                        $value["startingRemainderValueInUserCurrency"],
                        $value["startingRemainderValue"]
                    ]
                ];
            }
        } 

        return $data;
    }

    /**
     * example:
     * @return:
     * [
     *  "company1" => ["account1, "account2", ..],
     *  "company2" => [..]
     * ]
     */
    public function getCompaniesAccounts(array &$accountsInfo)
    {
        $data = [];
        foreach ($accountsInfo["items"] as $item) {
            $titleAccount = $item["title"];
            $titleCompany = $item["company"]["title"];
            if (!isset($data[$titleCompany])) {
                $data[$titleCompany] = [];
            }
            $data[$titleCompany][] = $titleAccount;
        }
        return $data;
    }

    public function getCompaniesBalance(array $companiesAccounts, array &$accountsBalance): array
    {
        $data = [];
        foreach (array_keys($accountsBalance) as $account) {
            foreach (array_keys($companiesAccounts) as $company) {
                if (!in_array($account, $companiesAccounts[$company])) {
                    continue;
                }
                
                if (!isset($data[$company])) {
                    $data[$company] = [];
                }
                
                foreach (array_keys($accountsBalance[$account]) as $date) {
                    if (!isset($data[$company][$date])) {
                        $data[$company][$date] = [0.0, 0.0];
                    }
                    
                    $balanceRub = $accountsBalance[$account][$date][0];
                    $balanceUsd = $accountsBalance[$account][$date][1];
                    
                    $data[$company][$date][0] += $balanceRub;
                    $data[$company][$date][1] += $balanceUsd;
                }
            }
        }
        return $data;
    }

    /**
     * get starting values by accounts
     */
    public function concatAccountsStartingValues()
    {
        $accounts = $this->getAccounts();
        $startingValues = $this->getAccountsStartingValues($accounts);
        return $startingValues;
    }

    public function delSpacesBalance(array &$accountsBalance)
    {
        $dateFrom = "2020-01-01";
        $dateTo = date("Y-m-d", time());
        foreach ($accountsBalance as &$accountBalance) {
            $newData = [];
            $dates = array_keys($accountBalance);
            //заполнить пробелы внутри массива
            for($i = 1; $i < count($accountBalance); ++$i) {
                $datePrev = $dates[$i-1];
                $dateCurrent = $dates[$i];
                $date = date("Y-m-d", strtotime($datePrev) + 60 * 60 * 24);
                while ($date < $dateCurrent) {
                    $newData[$date] = $accountBalance[$datePrev];
                    $date = date("Y-m-d", strtotime($date) + 60 * 60 * 24);
                }
                $newData[$datePrev] = $accountBalance[$datePrev];
                $newData[$dateCurrent] = $accountBalance[$dateCurrent];
            }  

            //заполнить пробелы слева и справа массива
            $date = $dateFrom;
            $newDates = array_keys($newData);
            while ($date <= $dateTo) {
                if (in_array($date, $newDates)) {
                    $date = date("Y-m-d", strtotime($date) + 60 * 60 * 24);
                    continue;
                }
                $value = $date < $dates[0] ? [0.0, 0.0] : $accountBalance[$dates[count($dates)-1]];
                $newData[$date] = $value;
                $date = date("Y-m-d", strtotime($date) + 60 * 60 * 24);

            }
            ksort($newData);
            $accountBalance = $newData;
        }
    }

    public function test()
    {
        
    }
}