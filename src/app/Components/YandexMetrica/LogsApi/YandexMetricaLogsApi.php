<?php

namespace app\Components\YandexMetrica\LogsApi;

use GuzzleHttp\Client;

class YandexMetricaLogsApi
{
    /*
    * DOCS - https://yandex.ru/dev/metrika/doc/api2/concept/about.html
    */

    private $client;
    private $uri;
    private $token; 
    private $options; 

    public function __construct()
    {
        $this->client = new Client;
        $this->uri = "https://api-metrika.yandex.net/management/v1/counter/";
        $this->token = env("YANDEX_METRICA_TOKEN");
        $this->options = [
            "headers" => [
                "Authorization" => "OAuth " . $this->token,
                "Content-Type" => "application/x-yametrika+json"
            ],
        ]; 
    }

    public function getNamesYmVisits(): array
    {
        return ['ym:s:visitID','ym:s:counterID','ym:s:watchIDs','ym:s:date','ym:s:dateTime','ym:s:dateTimeUTC','ym:s:isNewUser','ym:s:startURL','ym:s:endURL','ym:s:pageViews','ym:s:visitDuration','ym:s:bounce','ym:s:ipAddress','ym:s:regionCountry','ym:s:regionCity','ym:s:regionCountryID','ym:s:regionCityID','ym:s:clientID','ym:s:networkType','ym:s:goalsID','ym:s:goalsSerialNumber','ym:s:goalsDateTime','ym:s:goalsPrice','ym:s:goalsOrder','ym:s:goalsCurrency','ym:s:lastTrafficSource','ym:s:lastAdvEngine','ym:s:lastReferalSource','ym:s:lastSearchEngineRoot','ym:s:lastSearchEngine','ym:s:lastSocialNetwork','ym:s:lastSocialNetworkProfile','ym:s:referer','ym:s:lastDirectClickOrder','ym:s:lastDirectBannerGroup','ym:s:lastDirectClickBanner','ym:s:lastDirectClickOrderName','ym:s:lastClickBannerGroupName','ym:s:lastDirectClickBannerName','ym:s:lastDirectPhraseOrCond','ym:s:lastDirectPlatformType','ym:s:lastDirectPlatform','ym:s:lastDirectConditionType','ym:s:lastCurrencyID','ym:s:from','ym:s:UTMCampaign','ym:s:UTMContent','ym:s:UTMMedium','ym:s:UTMSource','ym:s:UTMTerm','ym:s:openstatAd','ym:s:openstatCampaign','ym:s:openstatService','ym:s:openstatSource','ym:s:hasGCLID','ym:s:lastGCLID','ym:s:firstGCLID','ym:s:lastSignificantGCLID','ym:s:browserLanguage','ym:s:browserCountry','ym:s:clientTimeZone','ym:s:deviceCategory','ym:s:mobilePhone','ym:s:mobilePhoneModel','ym:s:operatingSystemRoot','ym:s:operatingSystem','ym:s:browser','ym:s:browserMajorVersion','ym:s:browserMinorVersion','ym:s:browserEngine','ym:s:browserEngineVersion1','ym:s:browserEngineVersion2','ym:s:browserEngineVersion3','ym:s:browserEngineVersion4','ym:s:cookieEnabled','ym:s:javascriptEnabled','ym:s:screenFormat','ym:s:screenColors','ym:s:screenOrientation','ym:s:screenWidth','ym:s:screenHeight','ym:s:physicalScreenWidth','ym:s:physicalScreenHeight','ym:s:windowClientWidth','ym:s:windowClientHeight','ym:s:purchaseID','ym:s:purchaseDateTime','ym:s:purchaseAffiliation','ym:s:purchaseRevenue','ym:s:purchaseTax','ym:s:purchaseShipping','ym:s:purchaseCoupon','ym:s:purchaseCurrency','ym:s:purchaseProductQuantity','ym:s:productsPurchaseID','ym:s:productsID','ym:s:productsName','ym:s:productsBrand','ym:s:productsCategory','ym:s:productsCategory1','ym:s:productsCategory2','ym:s:productsCategory3','ym:s:productsCategory4','ym:s:productsCategory5','ym:s:productsVariant','ym:s:productsPosition','ym:s:productsPrice','ym:s:productsCurrency','ym:s:productsCoupon','ym:s:productsQuantity','ym:s:impressionsURL','ym:s:impressionsDateTime','ym:s:impressionsProductID','ym:s:impressionsProductName','ym:s:impressionsProductBrand','ym:s:impressionsProductCategory','ym:s:impressionsProductCategory1','ym:s:impressionsProductCategory2','ym:s:impressionsProductCategory3','ym:s:impressionsProductCategory4','ym:s:impressionsProductCategory5','ym:s:impressionsProductVariant','ym:s:impressionsProductPrice','ym:s:impressionsProductCurrency','ym:s:impressionsProductCoupon','ym:s:offlineCallTalkDuration','ym:s:offlineCallHoldDuration','ym:s:offlineCallMissed','ym:s:offlineCallTag','ym:s:offlineCallFirstTimeCaller','ym:s:offlineCallURL','ym:s:parsedParamsKey1','ym:s:parsedParamsKey2','ym:s:parsedParamsKey3','ym:s:parsedParamsKey4','ym:s:parsedParamsKey5','ym:s:parsedParamsKey6','ym:s:parsedParamsKey7','ym:s:parsedParamsKey8','ym:s:parsedParamsKey9','ym:s:parsedParamsKey10'];
    }

    public function getNamesYmHits(): array
    {
        return ['ym:pv:watchID','ym:pv:counterID','ym:pv:date','ym:pv:dateTime','ym:pv:title','ym:pv:URL','ym:pv:referer','ym:pv:UTMCampaign','ym:pv:UTMContent','ym:pv:UTMMedium','ym:pv:UTMSource','ym:pv:UTMTerm','ym:pv:browser','ym:pv:browserMajorVersion','ym:pv:browserMinorVersion','ym:pv:browserCountry','ym:pv:browserEngine','ym:pv:browserEngineVersion1','ym:pv:browserEngineVersion2','ym:pv:browserEngineVersion3','ym:pv:browserEngineVersion4','ym:pv:browserLanguage','ym:pv:clientTimeZone','ym:pv:cookieEnabled','ym:pv:deviceCategory','ym:pv:from','ym:pv:hasGCLID','ym:pv:GCLID','ym:pv:ipAddress','ym:pv:javascriptEnabled','ym:pv:mobilePhone','ym:pv:mobilePhoneModel','ym:pv:openstatAd','ym:pv:openstatCampaign','ym:pv:openstatService','ym:pv:openstatSource','ym:pv:operatingSystem','ym:pv:operatingSystemRoot','ym:pv:physicalScreenHeight','ym:pv:physicalScreenWidth','ym:pv:regionCity','ym:pv:regionCountry','ym:pv:regionCityID','ym:pv:regionCountryID','ym:pv:screenColors','ym:pv:screenFormat','ym:pv:screenHeight','ym:pv:screenOrientation','ym:pv:screenWidth','ym:pv:windowClientHeight','ym:pv:windowClientWidth','ym:pv:lastTrafficSource','ym:pv:lastSearchEngine','ym:pv:lastSearchEngineRoot','ym:pv:lastAdvEngine','ym:pv:artificial','ym:pv:pageCharset','ym:pv:isPageView','ym:pv:link','ym:pv:download','ym:pv:notBounce','ym:pv:lastSocialNetwork','ym:pv:httpError','ym:pv:clientID','ym:pv:networkType','ym:pv:lastSocialNetworkProfile','ym:pv:goalsID','ym:pv:shareService','ym:pv:shareURL','ym:pv:shareTitle','ym:pv:iFrame','ym:pv:parsedParamsKey1','ym:pv:parsedParamsKey2','ym:pv:parsedParamsKey3','ym:pv:parsedParamsKey4','ym:pv:parsedParamsKey5','ym:pv:parsedParamsKey6','ym:pv:parsedParamsKey7','ym:pv:parsedParamsKey8','ym:pv:parsedParamsKey9','ym:pv:parsedParamsKey10'];
    }

    public function request(string $method, string $uri, array $options = [])
    {
        return ($this->client->request($method, $uri, $options))->getBody()->getContents();
    }


    private function setOptions(string $date1, string $date2, string $fields, string $source)
    {
        $options = $this->options;
        $options["logrequest"]["date1"] = $date1;
        $options["logrequest"]["date2"] = $date2;
        $options["logrequest"]["fields"] = $fields;
        $options["logrequest"]["source"] = $source;
        return $options;
    }

    /**
     * DOCS https://yandex.ru/dev/metrika/doc/api2/logs/queries/createlogrequest.html
     * 
     * example options for requests:
     * $options = [
     *   "logrequest" => [
     *       "date1"=> "",
     *       "date2"=> "",
     *       "fields"=> "",
     *       "source"=> "",
     *   ],
     *   "headers" => [
     *       "Authorization" => "OAuth " . $token,
     *       "Content-Type" => "application/x-yametrika+json"
     *   ], 
     * ];
     * 
     * example requests:
     * POST https://api-metrika.yandex.net/management/v1/counter/{counterId}/logrequests?date1=2016-01-01&date2=2016-01-31&fields=ym:pv:dateTime,ym:pv:referer&source=hits
     * 
     * @param $method - default = "post" to create request
     * @param $counter - id of counter yandex.metrica
     * @param $date1 - first date (example 2020-12-31)
     * @param $date2 - second date (example 2022-01-01)
     * @param $fields - utm params, look docs 
     * @param $source - hits or visits, look docs
     * @return id of created log, needs to download data by this id 
     */
    public function createLogsRequests(string $counter, string $date1, string $date2, string $fields, string $source)
    {
        $uri = $this->uri . $counter . "/logrequests?date1=" . $date1 . "&date2=" . $date2 . "&fields=" . $fields . "&source=" . $source;
        $res = json_decode($this->request("post", $uri, $this->options), true);
        $request_id = $res["log_request"]["request_id"];

        if ($this->checkProcessed($counter, $request_id) === true);
        {
            return $request_id;
        }
            
        return null;
    }
    
    /**
     * Check possibility to download data
     * @param string $counter - id of counter yandex.metrica
     * @param string $id - id of created log
     * @return bool: true - can download, false - can't download
     */
    public function checkProcessed(string $counter, string $id): bool
    {
        while (true)
        {
            $res = $this->request("get", $this->uri . $counter . "/logrequest/" . $id, $this->options);
            $res = json_decode($res, true);
        
            if ($res["log_request"]["status"] === "processed")
            {
                return true;
            }

            sleep(60);
        }
        return false;
    }

    /**
     * Download data by logs ids
     * @param array $ids - array of logs ids
     * @param string $counter - id of yandex.metrica counter
     * @param int $countFields - the number of fields on which the request was made in $this->createLogsRequests
     * @return array 2d array of downloaded data 
     */
    public function downloadData($id, string $counter, int $countFields)
    {
        $objs = [];
        
        $res = $this->request("get", $this->uri . $counter . "/logrequest/" . $id . "/part/0/download?", $this->options);
        $str = explode("\n", $res);
        foreach($str as $line)
        {
            $tmp = [];
            if(!empty($line))
            {
                $list = explode("\t", $line);
                for ($i = 0; $i < $countFields; ++$i)
                    $tmp[] = $list[$i] ?? "";
                $objs[] = $tmp;
            }
        }
        
        return $objs;
    }
}