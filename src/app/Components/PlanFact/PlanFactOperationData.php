<?php

namespace app\Components\PlanFact;

use App\Components\PlanFact\PlanFactIntegration;
use App\Components\Bigquery\BigqueryIntegration;

class PlanFactOperationData extends PlanFactIntegration
{
    /**
     * Проверка на статьи Доходы, Расходы
     * @param array $articles - [["Расходы"], ["Доходы"], ...]
     * @return true, если статья относится к Доходам, Расходам
     * @return false в противном случае
     */
    private function isIncomeOutcome(array $articles): bool 
    {
        $checkedArticles = ["Доходы", "Расходы"];

        foreach ($articles as $article)
        {
            foreach ($article as $subArticle)
            {
                if (in_array($subArticle, $checkedArticles))
                    return true;
            }
        }
        
        return false;
    }

    /**
     * get company title by account title
     */
    public function getCompanyTitle(array $accountTitle, array $companiesAccounts, array $stableField)
    {
        $company = [null];
        foreach (array_keys($companiesAccounts) as $companyTitle) {
            if (in_array($accountTitle[0], $companiesAccounts[$companyTitle])) {
                $company = [$companyTitle];
                break;
            }
        }

        $this->dublicateFields($company, $stableField);

        return $company;
    }
    
    private function dublicateFields(array &$field, array &$stableField)
    {
        if (count($field) >= count($stableField))
            return;

        while (count($field) !== count($stableField))
            $field[] = $field[0];
    }

    /**
     * 
     */
    private function AccountTitle(array &$data, array &$stableField)
    {
        $account = parent::getAccountTitle($data);
        $this->dublicateFields($account, $stableField);
        return $account;
    }

    /**
     * 
     */
    private function OperationType(array &$data, array &$stableField)
    {
        $type = parent::getOperationType($data);
        $this->dublicateFields($type, $stableField);
        return $type;
    }

    /**
     * 
     */
    private function Comment(array &$data, array &$stableField)
    {
        $comment = parent::getComment($data);
        $this->dublicateFields($comment, $stableField);
        return $comment;
    }

    /**
     * get all fields about operations
     * @param array &$responce - ref, result of $this->getOperations
     * @return array of objects
     */
    public function getData(string $date_start, string $date_end)
    {
        $res = parent::getOperations($date_start, $date_end)["data"]["items"];
        return $res;
    }

    public function balanceToArray(array &$accountsBalance, array &$dates, array &$account)
    {
        $date = $dates[0];
        $accountTitle = $account[0];

        $remainder = [$accountsBalance[$accountTitle][$date][0]];

        $this->dublicateFields($remainder, $dates);

        return $remainder;
    }

    public function getCompanyBalance(array $date, array $companyTitle, array $companiesBalance)
    {
        $balance = [$companiesBalance[$companyTitle[0]][$date[0]][0]];
        $this->dublicateFields($balance, $date);
        return $balance;
    }

    public function isNewIncome(array &$articles, array $stableField)
    {
        $flag = [false];
        foreach ($articles as &$article) {
            for ($i = 0; $i < count($article); ++$i) {
                if ($article[$i] === "Чистая прибыль по товарам" or $article[$i] === "Продажа товаров") {
                    unset($article[$i]);
                    $flag = [true];
                    break;
                }
            } 
        }
        $this->dublicateFields($flag, $stableField);
        return $flag;
    }
 
    /**
     * concat all attributes, union parts in objects
     * @param dates
     * @return array of objects like [[col1, col2, col3...], [col1, col2, col3...], [col1, col2, col3...]]
     */
    public function getDataApi(string $date_start, string $date_end)
    {
        $data = $this->getData($date_start, $date_end);
        $changes = parent::getAccountsHistory();
        $startingValues = parent::concatAccountsStartingValues();
        $accountsInfo = parent::getAccounts();
        $companiesAccounts = parent::getCompaniesAccounts($accountsInfo); 
        $accountsBalance = parent::getChangesBalance($changes, $startingValues);
        parent::delSpacesBalance($accountsBalance);
        $companiesBalance = parent::getCompaniesBalance($companiesAccounts, $accountsBalance);

        $objs = [];
        foreach ($data as $row)
        {
            $category = parent::getCategory($row);
            if ($this->isIncomeOutcome($category) === false)
                continue;
            parent::unsetExcessArticle($category, ["Доходы", "Расходы"]);

            $date = parent::getDate($row);
            $account = $this->AccountTitle($row, $date);
            $type = $this->OperationType($row, $date);
            $contrAgent = parent::getContrAgent($row);
            $comment = $this->Comment($row, $date);
            $project = parent::getProject($row);
            $value = parent::getValue($row);
            $remainderAccount = $this->balanceToArray($accountsBalance, $date, $account);
            $company = $this->getCompanyTitle($account, $companiesAccounts, $date);
            $companyRemainder = $this->getCompanyBalance($date, $company, $companiesBalance);
            $isNetIncome = $this->isNewIncome($category, $date);
            for ($i = 0; $i < count($date); ++$i)
                $objs[] = $this->unionParts(
                    $i, $date, $account, $type, $contrAgent, 
                    $category, $category, $comment, $project, 
                    $value, $remainderAccount, $company, $companyRemainder,
                    $isNetIncome
                );
        }

        return $objs;
    }

    /**
     * union parts in objects
     * @param array &$date - array of date
     * @param array &$account
     * @param array &$type
     * @param array &$contrAgent
     * @param array &$comment
     * @param array &$value
     * @return array of objects ["col1", "col2", "col3"...]
     */
    public function unionParts(
        int $pos, array &$date, array &$account, array &$type, array &$contrAgent, 
        array &$category, array &$podCategory, array &$comment, array &$project, 
        array &$value, array &$remainder, array &$company, array &$companyRemainder,
        array &$isNetIncome
        ): array
    {
        $article = array_reverse($category[$pos]);
        return [
            "date" => $date[$pos],
            "account" => $account[$pos],
            "contragent" => $contrAgent[$pos],
            "type" => $type[$pos],
            "article" => $article[0],
            "sub_aricle_first" => $article[1] ?? null,
            "sub_aricle_second" => $article[2] ?? null,
            "sub_aricle_third" => $article[3] ?? null,
            "comment" => $comment[$pos],
            "project" => $project[$pos],            
            "value" => $value[$pos],
            "remainder" => $remainder[$pos],
            "company" => $company[$pos],
            "company_remainder" => $companyRemainder[$pos],
            "is_net_income" => $isNetIncome[$pos]
        ];
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