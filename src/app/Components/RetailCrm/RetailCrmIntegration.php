<?php

namespace app\Components\RetailCrm;

use GuzzleHttp\Client;

class RetailCrmIntegration
{
    /*
    * DOCS - https://help.retailcrm.ru/Developers/ApiRules
    */    

    private $guzzleClient;

    public function __construct()
    {
        $this->guzzleClient = new Client;
    }

    public function request(string $method, string $uri = "", array $params = [])
    {
        return json_decode(($this->guzzleClient->request($method, $uri, $params))->getBody()->getContents(), true);
    }

    /**
     * узнать количество страниц для пагинации
     */
    public function totalPageCount(string $mode, string $uri, array $params, string $filters = "")
    {
        $res = $this->request("get", $uri . "$mode?$filters&limit=100", $params);

        return $res["pagination"]["totalPageCount"];
    }

    /**
     * Собрать информацию о каждом сайте
     */
    public function getSitesInfo(string $uri, array $params)
    {
        $res = $this->request("get", $uri . "reference/sites", $params);
        
        foreach($res["sites"] as $site)
            $sites[] = $site;
        
        return $sites;
    }

    /**
     * Найти название сайта
     * @param $sites = $this->getSitesInfo
     * @return ?string название сайта или null
     */
    public function getSiteName(array $sites, string $siteCode): ?string
    {
        foreach ($sites as $site) 
            if ($site["code"] === $siteCode) 
                return $site["name"];
    
        return null;
    }
    

    /**
     * Получить имя менеджера (например, нужно, чтобы раскрыть имя человека, сменившего статус заказа)
     * @param $managerId
     * @return string имя менеджера
     */
    public function getManagerName(array $users, ?int $managerId)
    {
        if ($managerId === null)
            return null;
        
        foreach(array_keys($users) as $user)
        {
            if ($managerId == $user)
                return $users[$managerId];
        }

        return $managerId;
    }

    /**
     * Получить список пользователей
     * @return array [id => name]
     */
    public function getUsers(string $uri, array $params)
    {
        $mode = "users";
        $totalPageCount = $this->totalPageCount($mode, $uri, $params);

        $users = [];
        for ($page = 1; $page <= $totalPageCount; ++$page)
        {
            $res = $this->request("get", $uri . "$mode?limit=100&page=$page", $params);

            foreach ($res["users"] as $user)
            {
                $firstName = $user['firstName'] ?? null;
                $lastName = $user['lastName'] ?? null;
                $fio = [$firstName, $lastName];

                $users[$user["id"]] = implode(" ", $fio);
            }
        }

        return $users;
    }

    public function getStatusesCategory()
    {
        return [
            "new" => ["new"],
            "soglasovanie" => ["podtvedit-oplatu", "nedozvon-novyi-status", "predzakaz", "perezvonit-segodnia", "otlozhenniy-zakaz", "pozvonit-klientu"],
            "soglasovanienasklade" => ["offer-analog", "soglasovanie-na-sklade", "otkaz-pri-zvonke-kurera", "predlozhit-zamenu"],
            "assembling" => ["assembly", "assembling", "assembling-complete", "komplektuetsia-vozvrat", "prosro4kacourier", "prosro4kasdek", "ready"],
            "delivery" => ["posylka-edet-nazad", "send-to-delivery", "vypolnen-i-ne-proveden", "dostavlyaetsya-po-msk", "vozvrat-posylki", "dostavlyaetsya-po-rf", "dostavlyaetsya1mig", "pribylo-na-mesto-vrucheniya", "trebuet-prorabotki"],
            "completed" => ["complete"],
            "trade" => ["obmen"],
            "canceled" => ["cancel-other", "oformlen-vozvrat", "uteryan-pochtoy-rf", "otkaz-pri-zvonke", "nedozvon-2ur", "no-product", "nekachestvennaia-zaiavka", "pribylo-na-mesto-vrucheniya", "otkaz-pri-dostavke", "ne-original", "kopiia-zakaza", "otmenen-huligany-test-i-dr", "ne-ustroila-dostavka", "net-v-nalichii-promezhutochnyi"]
        ];
    }

    /**
     * Получить корректные имена всех статусов
     */
    public function getStatusesInfo(string $uri, array $params)
    {
        $mode = "reference/statuses";

        $res = $this->request("get", $uri . $mode, $params);

        if ($res["success"])
            return $res["statuses"];

        return null;
    }

    /**
     * Получить корректное имя статуса
     * @param $this->getStatusesInfo
     * @param $statusCode - транслитом написанный статус 
     */
    public function parseStatusByCode(array $statuses, string $statusCode)
    {
        return $statuses[$statusCode]["name"] ?? null;
    }

    /**
     * Получить статус (?)
     */
    public function getProductStatuses(string $uri, array $params)
    {
        $mode = "reference/product-statuses";

        $res = $this->request("get", $uri . $mode, $params);
        
        if ($res["success"])
            return $res["productStatuses"];

        return null;
    }

    /**
     * Получить статус (?)
     */
    public function parseProductStatuses(array $statuses, string $statusCode)
    {
        foreach ($statuses as $item) 
        {
            if ($item["code"] == $statusCode) 
                return $item["name"];
        }

        return null;
    }

    /**
     * Получить статус (?)
     */
    public function getProductsInfo(string $uri, array $params, $filters = "")
    {
        //$mode = "store/products";
        $mode = "reference/stores";
        //$totalPageCount = $this->totalPageCount($mode, $uri, $params, $filters);
        //dd($totalPageCount);
        $info = [];
        for ($page = 1; $page < 2; ++$page)
        {
            $res = $this->request("get", $uri . "$mode?$filters", $params);
            dd($res);
            foreach ($res["products"] as $product)
            {
                if ($product["name"] === "НАШ ТОВАР")
                {
                    dd($product);
                }
                $info[] = $product;
            }
                
        }

        return $info;
    }

    public function packs(string $uri, array $params, $id)
    {
        $mode = "orders/packs";

        $res = $this->request("get", $uri . $mode . "?filter[orderId]=$id", $params);

        if ($res["success"])
            return $res["packs"];

        return null;
    }
    
}