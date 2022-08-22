<?php

/** @var \Laravel\Lumen\Routing\Router $router */

/*
|--------------------------------------------------------------------------
| Application Routes
|--------------------------------------------------------------------------
|
| Here is where you can register all of the routes for an application.
| It is a breeze. Simply tell Lumen the URIs it should respond to
| and give it the Closure to call when that URI is requested.
|
*/

use Carbon\Carbon;
use GuzzleHttp\Client;
use app\Dto\TildaRowDtoLog;
use app\Dto\TildaRowDtoData;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\App;
use App\Components\RetailCrm\RetailCrmMainData;
use App\Components\Bigquery\BigqueryIntegration;
use App\Components\Bigquery\BigqueryOperationData;
use App\Components\Bigquery\BigqueryActualRemainder;
use App\Components\PlanFact\PlanFactOperationData;
use App\Components\PlanFact\PlanFactActualRemainder;
use App\Dto\YandexMetricaRowDto\YandexMetricaRowDto;
use App\Components\RetailCrm\RetailCrmHistoryStatuses;
use App\Components\GoogleSheets\GoogleSheetsIntegration;
use App\Components\YandexMetrica\LogsApi\YandexMetricaLogsApi;
use App\Components\YandexMetrica\Reports\YandexMetricaReports;

ini_set('memory_limit', '512M');
set_time_limit(36000);
//ini_set('post_max_size', '1024M');

$router->get('/', function () use ($router) {
    return $router->app->version();
});

$router->get('yandex-metrica-reports', function (YandexMetricaReports $ya)
{
    $data = $ya->handle();
    $bq = new BigqueryIntegration("decoded-academy-342708");
    $res = $bq->insertRows("yandex_metrica", "data", $data);
    if ($res->isSuccessful()) {
        return 1;
    } else {
        
        return 0;
    }
});

$router->get('yandex-metrica-reports-test', function (Request $request, YandexMetricaReports $ya)
{
    // $date = date("Y-m-d", time()-(60*60*24));

    // запрос на получение счетчиков
    $counters = $ya->getInfoCounters()->counters;

    // собираю id счетчиков
    $ids = $ya->getIdCounters($counters);
    
    $logins = $ya->getOwnerLogin($counters);
    
    $names = $ya->getCountersName($counters);
    
    $urls = $ya->getUrlsSite($counters);

    $objs = [];
    $date = "2022-03-30";
    while ($date != "2022-06-07")
    {
        for ($index = 0; $index < count($counters); ++$index)
        {
            $stats = $ya->getStats($ids[$index], $date, $date, "ym:s:visits,ym:s:pageviews,ym:s:users");

            $date = $date ?? "";
            $id_counter = $ids[$index] ?? "";
            $owner_counter = $logins[$index] ?? "";
            $name_counter = $names[$index] ?? "";
            $uri_site = $urls[$index] ?? "";
            $visits = $stats[0] == "" ? 0 : $stats[0];
            $views = $stats[1] ?? 0;
            $users = $stats[2] ?? 0;

            $data = [
                "date" => $date,
                "id_counter" => $id_counter,
                "owner_counter" => $owner_counter,
                "name_counter" => $name_counter,
                "uri_site" => $uri_site,
                "views" => $views,
                "visits" => $visits,
                "users" => $users,
            ];
            $objs[] = $data;
        }
        $timestamp = strtotime($date);
        $date = date("Y-m-d", $timestamp + (24*60*60));
    }

    $bq = new BigqueryIntegration("decoded-academy-342708");
    $res = $bq->insertRows("yandex_metrica", "data", $objs);
    if ($res->isSuccessful()) {
        return 1;
    } else {
        return 0;
    }
});

$router->get('retailcrm-history-statuses', function (RetailCrmHistoryStatuses $retailcrm, Request $request) 
{
    $startDate = date("Y-m-d 00:00:00", time() - 60 * 60 * 48);
    $endDate = date("Y-m-d 00:00:00");

    $rows = $retailcrm->getHistory($startDate, $endDate);

    $normalizedRows = $retailcrm->concatChanges($rows);
    $retailcrm->setCorrectOrderId($normalizedRows);

    foreach(array_chunk($normalizedRows, 500) as $Rows)  {
        $retailcrm->loadHistoryBq("decoded-academy-342708", "retail_crm", "history_statuses", $Rows);
    }
        
    return 1;
});

$router->get('retailcrm-main-data', function (RetailCrmMainData $retailcrm) 
{
    $bq = new BigqueryIntegration("decoded-academy-342708");

    $startTime = date("Y-m-d 00:00:00",time() - 60 * 60 * 24 * 50);

    $res = $retailcrm->getOrders($startTime);

    $bq->query("DELETE FROM `decoded-academy-342708.retail_crm.data_main` WHERE created_date > '$startTime';");
    
    foreach(array_chunk($res, 500) as $rows)
        $bq->insertRows("retail_crm", "data_main", $rows);

    return 1;
});

$router->get('plan-fact-operation-data', function () 
{
    $pf = new PlanFactOperationData;
    $bq = new BigqueryOperationData("decoded-academy-342708", "plan_fact", "operation_data");
   
    $date_start = date("Y-m-d", time() - 60 * 60 * 1440);
    $date_end = date("Y-m-d", time());

    $dataApi = $pf->getDataApi($date_start, $date_end); 

    $bq->clearByDateStart($date_start);

    $res = $bq->insertData($dataApi);

    if ($res->isSuccessful()) {
        return 1;
    } else {
        return 0;
    }
});

$router->get('plan-fact-actual-remainder', function () 
{
    $pf = new PlanFactActualRemainder;
    $bq = new BigqueryActualRemainder("decoded-academy-342708", "plan_fact", "actual_remainder");

    $rowsApi = $pf->getData();
    dd($pf->saveCsv($rowsApi, '/Users/vovalemba/Work_project/internet_magazin_integration_git/internet-magazin-integration/src/actual_remainder.csv'));
    $rowsBq = $bq->getData();

    $accountsBq = $pf->getAccountsBq($rowsBq);
    $accountsApi = $pf->getAccountsApi($rowsApi);
     
    $accountsToDel = $pf->getOldAccounts($accountsBq, $accountsApi);

    $accountsToAdd = $pf->getNewAccounts($accountsBq, $accountsApi);
    $accountsToUpdate = $pf->getUpdatesAccount($accountsToAdd, $accountsToDel, $accountsApi);

    $rowsToAdd = $pf->getRowsToManipulate($accountsToAdd, $rowsApi);
    $rowsToUpdate = $pf->getRowsToManipulate($accountsToUpdate, $rowsApi);

    $bq->queryDelRows($accountsToDel);
    $bq->queryAddRows($rowsToAdd);
    $res = $bq->queryUpdateAccounts($rowsToUpdate);

    if ($res === []) {
        return 1;
    } else {
        return 0;
    }
});

