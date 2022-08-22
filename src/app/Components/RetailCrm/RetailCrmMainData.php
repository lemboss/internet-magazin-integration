<?php

namespace app\Components\RetailCrm;

use App\Components\Bigquery\BigqueryIntegration;
use App\Components\RetailCrm\RetailCrmIntegration;

class RetailCrmMainData
{
    private $retailcrm;
    private $token;
    private $uri;
    private array $headers;
    private array $schemaBigqueryTable;

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
                ['name' => 'order_id', 'type' => 'INTEGER'],
                ['name' => 'order_number', 'type' => 'STRING'],
                ['name' => 'manager_name', 'type' => 'STRING'],
                ['name' => 'customer', 'type' => 'STRING'],
                ['name' => 'shop_name', 'type' => 'STRING'],
                ['name' => 'order_price', 'type' => 'FLOAT'],
                ['name' => 'service_cost', 'type' => 'FLOAT'],
                ['name' => 'created_date', 'type' => 'DATETIME'],
                ['name' => 'solead', 'type' => 'STRING'],
                ['name' => 'delivery_service', 'type' => 'STRING'],
                ['name' => 'cdek_delivery_type', 'type' => 'STRING'],
                ['name' => 'country', 'type' => 'STRING'],
                ['name' => 'region', 'type' => 'STRING'],
                ['name' => 'city', 'type' => 'STRING'],
                ['name' => 'actual_order_status', 'type' => 'STRING'],
                ['name' => 'adv_source', 'type' => 'STRING'],
                ['name' => 'collector_name', 'type' => 'STRING'],
                ['name' => 'courier_name', 'type' => 'STRING'],
                ['name' => 'cost_price_delivery', 'type' => 'FLOAT'],
                ['name' => 'price_delivery', 'type' => 'FLOAT'],
                //['name' => 'description', 'type' => 'STRING'],
                ['name' => 'product_name', 'type' => 'STRING'],
                ['name' => 'article', 'type' => 'STRING'],
                ['name' => 'discount', 'type' => 'FLOAT'],
                ['name' => 'cost_price', 'type' => 'FLOAT'],
                ['name' => 'quantity', 'type' => 'FLOAT'],
                ['name' => 'product_price', 'type' => 'FLOAT'],
                ['name' => 'earnings', 'type' => 'FLOAT'],
                ['name' => 'offer_id', 'type' => 'INTEGER'],
                ['name' => 'product_status', 'type' => 'STRING'],
                ['name' => 'storage', 'type' => 'STRING'],
                ['name' => 'brand', 'type' => 'STRING'],
                ['name' => 'product_group', 'type' => 'STRING'],
                ['name' => 'paid_quantity', 'type' => 'FLOAT'],  
            ]
        ];
    }

    public function getOrders($startTime)
    {
        $users = $this->retailcrm->getUsers($this->uri, $this->headers); 
        $sites = $this->retailcrm->getSitesInfo($this->uri, $this->headers);
        $nameStatuses = $this->retailcrm->getStatusesInfo($this->uri, $this->headers);
        $productStatuses = $this->retailcrm->getProductStatuses($this->uri, $this->headers);
        
        $pagesCount = $this->retailcrm->totalPageCount("orders", $this->uri, $this->headers, "filter[createdAtFrom]=$startTime");
        
        $prevTotalSumm = 0;
        
        for ($page = 1; $page <= $pagesCount; ++$page)
        {
            $res = $this->retailcrm->request("get", $this->uri . "orders?limit=100&page=$page&filter[createdAtFrom]=$startTime", $this->headers);

            foreach ($res["orders"] as $item)
            {
                if (count($item["items"]) === 0)
                    continue;
                
                $orders = [
                    'order_id' => $item["id"] ?? null,
                    'order_number' => $item["number"],
                    'manager_name' => isset($item["managerId"]) ? $this->retailcrm->getManagerName($users, $item["managerId"]) : null,
                    'customer' => $item["firstName"] ?? null,
                    'shop_name' => isset($item["site"]) ? $this->retailcrm->getSiteName($sites, $item["site"]) : null,
                    'order_price' => isset($item["totalSumm"]) ? (float)$item["totalSumm"] : null,
                    'service_cost' => isset($item["customFields"]["partnerservicecost"]) ? (float)
                                           $item["customFields"]["partnerservicecost"] / count($item["items"]) : null,
                    'created_date' => $item["createdAt"],
                    'solead' => stristr($this->retailcrm->getSiteName($sites, $item["site"]), 'solead') ? 'TRUE' : 'FALSE',
                    'delivery_service' => null,
                    'cdek_delivery_type' => null,
                    'country' => $item["delivery"]["address"]["countryIso"] ?? null,
                    'region' => $item["delivery"]["address"]["region"] ?? null,
                    'city' => $item["delivery"]["address"]["city"] ?? null,
                    'actual_order_status' => $this->retailcrm->parseStatusByCode($nameStatuses, $item["status"]),                 
                    'adv_source' => $item["customFields"]["cltchefo_fe2i"] ?? null,
                    "collector_name" => null,
                    "courier_name" => $item["delivery"]["data"]["firstName"] ?? null,
                    "cost_price_delivery" => (float)$item["delivery"]["netCost"] ?? null,
                    "price_delivery" => (float)$item["delivery"]["cost"] ?? null,
                    //"description" => null //это мб нужно удалить
                ];
      
                //имя сборщика
                $value = $item["id"];
                $totalPageCount = $this->retailcrm->totalPageCount("orders/history", $this->uri, $this->headers, "filter[orderId]=$value");
                $responce = $this->retailcrm->request("get", $this->uri . "orders/history?page=$totalPageCount&limit=100&filter[orderId]=$value", $this->headers);
                for ($i = 1; $i < count($responce["history"]); ++$i)
                {
                    $data = $responce["history"][$i];
                    if ($data["order"]["status"] !== "assembling-complete")
                        continue;

                    $user = $data["user"] ?? null;
                    if ($user === null)
                        continue;

                    $orders["collector_name"] = $this->retailcrm->getManagerName($users, $user["id"]);    
                }       
                
                if (isset($item["delivery"]["code"])) 
                {
                    if ($item["delivery"]["code"] == 'cdek') 
                    {
                        $orders['delivery_service'] = 'СДЕК';
                        $orders['cdek_delivery_type'] = 'СДЕК';
                        if (isset($item["delivery"]["data"]["deliveryName"])) 
                        {
                            if (stristr($item["delivery"]["data"]["deliveryName"], 'Склад-Дверь')) 
                            {
                                $orders['cdek_delivery_type'] = 'Склад-Дверь';
                            } 
                            elseif (stristr($item["delivery"]["data"]["deliveryName"], 'Склад-Склад')) 
                            {
                                $orders['cdek_delivery_type'] = 'Склад-Склад';
                            } 
                            elseif (stristr($item["delivery"]["data"]["deliveryName"], 'Дверь-Склад')) 
                            {
                                $orders['cdek_delivery_type'] = 'Дверь-Склад';
                            } 
                            else                             
                            {
                                $orders['cdek_delivery_type'] = 'Дверь-Дверь';
                            }
                        }
                    }
                    elseif ($item["delivery"]["code"] == 'courier') 
                    {
                        $orders['delivery_service'] = 'Курьер';
                    } 
                } 
                else 
                {
                    $orders['delivery_service'] = null;
                }
    
                /*
                * В след. цикле добавляется информация по каждому товару к уже имеющейся инфе по самому заказу. Инфа по
                * заказу дублируется в каждую строчку, а инфа по товару будет в каждой строчке разная
                */
                $packs = $this->retailcrm->packs($this->uri, $this->headers, $orders["order_id"]);
                for ($i = 0; $i < count($item["items"]); ++$i) 
                {
                    $product = $item["items"][$i];
                    $products = [
                        'product_name' => $product["offer"]["name"] ?? null,
                        'article' => $product["offer"]["article"] ?? null,
                        'discount' => isset($product["discountTotal"]) ? (float)$product["discountTotal"] : null,
                        'cost_price' => isset($product["purchasePrice"]) ? (float)$product["purchasePrice"] : null,
                        'quantity' => isset($product["quantity"]) ? (float)$product["quantity"] : null,
                        'product_price' => isset($product["prices"][0]["price"]) ? (float)$product["prices"][0]["price"] : null,
                        'earnings' => isset($item["customFields"]["partnerservicecost"]) ? 
                                            (float)($product["prices"][0]["price"] - $product["discountTotal"] -
                                            $item["customFields"]["partnerservicecost"] / count($item["items"])) : null,
                        'offer_id' => $product["offer"]["id"],
                        'product_status' => $this->retailcrm->parseStatusByCode($productStatuses, $product["status"]),
                        "storage" => $packs[$i]["store"] ?? null
                    ];

                    /*
                    * след. блок сделал по просьбе Айрата, чтобы он мог нормально считать суммы заказов. Так как товар в
                    * каждой строчке, то там и сумма заказа дублируется. Чтобы такого не происходило, обнуляем в каждой
                    * строчке, если товаров в заказе больше одного и данные по самому заказу копируются
                    */
                    if ($prevTotalSumm == $orders['order_price']) 
                    {
                        $prevTotalSumm = $orders['order_price'];
                        $orders['order_price'] = 0;
                    } 
                    else 
                    {
                        $prevTotalSumm = $orders['order_price'];
                    }
                    
                    $finalArray[] = $orders + $products;  // вот тут происходит слияние инфы по самому заказу 
                }        
            } 
        }
        
        /*
        * далее по коду идет определение принадлежности товара к группе "Обувь" или "Сумки" а так же определяется бренд.
        * Для этого нужно собрать все торговые предложения по всем товарам и парсить там ключевые слова. По ним и определять
        */

        foreach ($finalArray as $row) 
            $orderIds[] = $row['order_id'];
        
        foreach ($finalArray as $product) 
            $offerIds[] = $product['offer_id'];
        
        /*$namesAndDescriptions = $this->getNamesAndDescriptions($offerIds);

        foreach ($finalArray as $rowKey => $row) 
        {
            foreach ($namesAndDescriptions as $key => $value) 
            {
                if ($key == $row['offer_id'] && isset($namesAndDescriptions[$key][1])) 
                    $finalArray[$rowKey]['description'] = $namesAndDescriptions[$key][1];
            }
        }*/
        // словарик ключевых слов по каждому бренду
        $brandsDictionary = [
            'Yeezy' => ['Yeezy', '350', '450', 'Reflective'],
            'Marc Jacobs' => ['Marc Jacobs', 'SNAPSHOT', 'COCONUT', 'The Eclipse', 'MINI BOX', 'marcjacobs'],
            'Michael Kors' => ['Michael Kors', 'Hendrix', 'HALLY SMALL', 'CROSSBODY BAG', 'JET SET MEDIUM', 'SLATER MEDIUM',
                            'Erin'],
            'Coach' => ['Coach'],
            'Dior' => ['Dior', 'SADDLE', 'CARO'],
            'Pinko' => ['Pinko', 'MINI SQUARE', 'Love', 'EVERYDAY', 'INFINITY', 'SQUARE BAG', 'CLUTCH BAG FRAIMED'],
            'UGG' => ['Ugg', 'Classic Mini', 'Classic Short'],
            'Nike' => ['Nike'],
            'Prada' => ['Prada'],
            'Loro Piana' => ['Loro Piana']
        ];
        $productGroups = [
            'shoes' => ['Yeezy', 'UGG', 'Nike'],
            'bags' => ['Marc Jacobs', 'Michael Kors', 'Coach', 'Dior', 'Pinko']
        ];
        
        /*
        * Источниками данных для парса по ключевыем словам является description, product_name и shop_name
        */
        foreach ($finalArray as $rowKey => $row) 
        {
            $fullDescription = $row['shop_name'] . ' ' . $row['product_name'];  // подготавливаем строку для парса по ключам
            /*if (isset($row['description'])) 
                $fullDescription .= ' ' . $row['description'];*/
            
            foreach ($brandsDictionary as $brandName => $brandVariations) 
            {
                foreach ($brandVariations as $variation) 
                {
                    if (stristr($fullDescription, $variation)) 
                    {
                        $finalArray[$rowKey]['brand'] = $brandName;
                        if (in_array($brandName, $productGroups['bags'])) 
                        {
                            $finalArray[$rowKey]['product_group'] = 'Сумки';
                        } 
                        else 
                        {
                            $finalArray[$rowKey]['product_group'] = 'Обувь';
                        }
                        break 2;
                    } 
                    else 
                    {
                        $finalArray[$rowKey]['brand'] = null;
                        $finalArray[$rowKey]['product_group'] = null;
                    }
                }
            }
        }
        
        $finalArray = $this->countPaidQuantityInOrders($finalArray);  // считаем оплаченные товары в заказе
        
        return $finalArray;
    }

    private function getNamesAndDescriptions($offerIds)
    {
        $uniqueOfferIds = array_unique($offerIds);
        $chunkedUniqueOfferIds = array_chunk($uniqueOfferIds, 800); // опытным путем выяснил что пачки по 800 оптимально
        $namesAndDescriptions = [];
        $totalPageCount = 10000;
        for ($i = 0; $i <= count($chunkedUniqueOfferIds); $i++) 
        {
            if (!isset($chunkedUniqueOfferIds[$i])) 
                break;

            for ($page = 1; $page <= $totalPageCount; $page++) {
                $result = $this->getResource('store/products', $page, $chunkedUniqueOfferIds[$i]);
                foreach ($result->products as $product) {
                    $namesAndDescriptions[$product->offers[0]->id] = [
                        '0' => $product->name,
                        '1' => $product->description ?? null
                    ];
                }
                $totalPageCount = $result->pagination->totalPageCount;
            }
        }

        foreach ($namesAndDescriptions as $key => $value) 
        {
            if ($value[1] == null || empty($value[1])) 
                unset($namesAndDescriptions[$key][1]);
        }

        return $namesAndDescriptions;
    }

    /*
    * функция считает оплаченные товары в заказе
    */
    private function countPaidQuantityInOrders($finalRows)
    {
        $countInOrder = 0;
        for ($i = 0; !empty($finalRows[$i]) and $i + 1 != count($finalRows); $i++) 
        {
            if ($finalRows[$i]['product_status'] == 'Оплачен' || $finalRows[$i]['product_status'] == 'Оплатить') 
            {
                $countInOrder += $finalRows[$i]['quantity'];
                    if ($finalRows[$i]['order_number'] != $finalRows[$i+1]['order_number']) 
                    {
                        $finalRows[$i]['paid_quantity'] = (float)$countInOrder;
                        $countInOrder = 0;
                    } 
                    else 
                    {
                        $finalRows[$i]['paid_quantity'] = 0;
                    }
            } else {
                $finalRows[$i]['paid_quantity'] = 0;
                if (isset($finalRows[$i+1]['order_number'])) 
                {
                    if ($finalRows[$i]['order_number'] != $finalRows[$i+1]['order_number']) 
                    {
                        if ($countInOrder != 0) 
                            $finalRows[$i]['paid_quantity'] = (float)$countInOrder;
                        
                        $countInOrder = 0;
                    }
                }
            }
        }

        return $finalRows;
    }

    private function test(string $startTime)
    {
        $res = $this->retailcrm->request("get", $this->uri . "reference/sites", $this->headers);
        dd($res["sites"]["nike490"]);
        dd($this->retailcrm->totalPageCount("reference/sites", $this->uri, $this->headers, "filter[createdAtFrom]=$startTime"));
        $endTime = date("Y-m-d H:m:s", time());
        //dd($endTime);
        $totalPageCount = $this->retailcrm->totalPageCount("orders/history", $this->uri, $this->headers, "filter[startDate]=$startTime");
        //dd($totalPageCount);
        $responce = $this->retailcrm->request("get", $this->uri . "orders/history?page=1&limit=100&filter[startDate]=$startTime", $this->headers);
        dd($responce);
    }

    private function getResource($resourceName, $page = 1, $offerIds = [])
    {
        $rootUrl = 'https://rtl2.retailcrm.ru/api/v5/' . $resourceName . '?';
        $params = $this->setParams($resourceName, $page, $offerIds);  // настраиваем параметры в зависимости от ресурса обращения

        $result = $this->executive($rootUrl, $params);
        $result = json_decode($result);

        return $result;
    }

    /*
    * функция устанавливающая параметры и фильтры для того или иного получаемого ресурса
    * входные параметры: название ресурса, страница пагинации, набор ids для фильтра
    */

    function setParams($resourceName, $page, $offerIds)
    {
        $params = ['apiKey' => $this->token];
        if ($resourceName == 'users') {
            $params += [
                'limit' => 100,
                'page' => $page
            ];
        } elseif ($resourceName == 'orders') {
            $params += [
                'limit' => 100,
                'page' => $page,
    //            'filter' => [
    //                'createdAtFrom' => '2022-01-01'
    //            ]
            ];
        } elseif ($resourceName == 'store/products') {
            $params += [
                'limit' => 100,
                'page' => $page,
                'filter' => [
                    'offerIds' => $offerIds
                ]
            ];
        } elseif ($resourceName == 'reference/sites') {
            $params += [
                'limit =>' => 50
            ];
        } elseif ($resourceName == 'custom-fields') {
            $params += [
                'limit' => 100
            ];
        } elseif ($resourceName == 'store/product-group'){
            $params = [
                'limit' => 100,
                'page' => $page
            ];
        } elseif ($resourceName == 'reference/statuses') {
            $params += [
                'limit' => 100,
                'page' => $page,
                'externalIds' => $offerIds
            ];
        }

        return $params;
    }

    private function executive($queryUrl, $params, $requestMethod = 'GET')
    {
        $params = http_build_query($params);
        $curl = curl_init();

        if ($requestMethod == 'GET') {
            curl_setopt_array($curl, array(
                CURLOPT_SSL_VERIFYPEER => 0,
                CURLOPT_HEADER => 0,
                CURLOPT_RETURNTRANSFER => 1,
                CURLOPT_URL => $queryUrl . $params
            ));
        } else {
            curl_setopt_array($curl, array(
                CURLOPT_SSL_VERIFYPEER => 0,
                CURLOPT_HEADER => 0,
                CURLOPT_POST => 1,
                CURLOPT_RETURNTRANSFER => 1,
                CURLOPT_URL => $queryUrl . $params
            ));
        }
        $result = curl_exec($curl);
        curl_close($curl);

        return $result;
    }

    public function getSchemaBigqueryTable()
    {
        return $this->schemaBigqueryTable;
    }

    /**
     * Создает новую таблицу $tableName в проекте $projectId, в датасете $datasetId
     * @return имя созданной таблицы 
     */
    public function createTableBq(string $projectId, string $datasetId, string $tableName)
    {   
        return $this->initBigqueryClient($projectId)->createTable($datasetId, $tableName, $this->schemaBigqueryTable);
    }
}
