<?php

namespace app\Components\YandexMetrica\Reports;

use GuzzleHttp\Client;

class YandexMetricaReports
{
    private $client;
    private $uri;
    private $uriReports;
    private $uriGoals;
    private $token; 
    private $options; 

    public function __construct()
    {
        $this->client = new Client;
        $this->uri = "https://api-metrika.yandex.net/management/v1/counters/";
        $this->uriReports = "https://api-metrika.yandex.net/stat/v1/";
        $this->uriGoals = "https://api-metrika.yandex.net/management/v1/counter/";
        $this->token = env("YANDEX_METRICA_TOKEN");
        $this->options = [
            "headers" => [
                "Authorization" => "OAuth " . $this->token,
                "Content-Type" => "application/x-yametrika+json"
            ],
        ]; 
    }

    public function request(string $method, string $uri, array $options = [])
    {
        return ($this->client->request($method, $uri, $options))->getBody()->getContents();
    }

    public function getInfoCounters()
    {
        return json_decode($this->request("get", $this->uri, $this->options));
    }

    public function getIdCounters($stats)
    {
        $ids = [];
        foreach($stats as $stat)
            $ids[] = $stat->id;
        return $ids;
    }

    public function getOwnerLogin($stats)
    {
        $logins = [];
        foreach($stats as $stat)
            $logins[] = $stat->owner_login;
        return $logins;
    }

    public function getCountersName($stats)
    {
        $names = [];
        foreach($stats as $stat)
            $names[] = $stat->name;
        return $names;
    }

    public function getUrlsSite($stats)
    {
        $urls = [];
        foreach($stats as $stat)
            $urls[] = $stat->site;
        return $urls;
    }

    public function getNums($stats)
    {
        $nums = [];
        foreach($stats as $stat)
            $nums[] = preg_replace('/[^0-9]/', '', $stat);
        return $nums;
    }

    public function getStats(string $ids, string $date1, string $date2, string $metrics)    
    {        
        $uri = sprintf("data.csv?ids=%s&date1=%s&date2=%s&metrics=%s", $ids, $date1, $date2, $metrics);
        $stats = explode('\n', $this->request("get", $this->uriReports . $uri, $this->options))[0] ?? "";        
        $stats = explode("\n", $stats)[1];        
        $stats = explode(",", $stats);   
        return $this->getNums($stats);    
    }

    /**
     * https://yandex.ru/dev/metrika/doc/api2/management/goals/class_goale.html
     * @return Goal
     */
    public function getGoals(string $counter)
    {
        $uri = $this->uriGoals . $counter . "/goals";
        return json_decode($this->request("get", $uri, $this->options))->goals;
    }

    public function getIdsGoals(array $goals): array
    {
        $objs = [];
        foreach($goals as $goal)
            $objs[] = $goal->id;
        return $objs;
    }

    public function handle()
    {
        // запрос на получение счетчиков
        $counters = $this->getInfoCounters()->counters;
        
        // собираю id счетчиков
        $ids = $this->getIdCounters($counters);
        
        $logins = $this->getOwnerLogin($counters);
        
        $names = $this->getCountersName($counters);
        
        $urls = $this->getUrlsSite($counters);

        $dateFrom = date("Y-m-d", time()-(60 * 60 * 24));
        $dateTo = $dateFrom; 
 
        $objs = [];
        for ($index = 0; $index < count($counters); ++$index)
        {
            $stats = $this->getStats($ids[$index], $dateFrom, $dateTo, "ym:s:visits,ym:s:pageviews,ym:s:users");
            $date = $dateFrom === $dateTo ? $dateFrom : "";
            $id_counter = $ids[$index] ?? "";
            $owner_counter = $logins[$index] ?? "";
            $name_counter = $names[$index] ?? "";
            $uri_site = $urls[$index] ?? "";
            $visits = $stats[0] == "" ? 0 : $stats[0];
            $views = $stats[1] ?? 0;
            $users = $stats[2] ?? 0;

            $objs[] = [
                "date" => $date,
                "id_counter" => $id_counter,
                "owner_counter" => $owner_counter,
                "name_counter" => $name_counter,
                "uri_site" => $uri_site,
                "views" => $views,
                "visits" => $visits,
                "users" => $users,
            ];
        }
        return $objs;
    }
}

