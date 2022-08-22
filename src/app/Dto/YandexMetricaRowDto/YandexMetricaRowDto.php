<?php

namespace app\Dto\YandexMetricaRowDto;

use Spatie\DataTransferObject\DataTransferObject;

class YandexMetricaRowDto extends DataTransferObject
{
    public string $date;
    public string $id_counter;
    public string $owner_counter;
    public string $name_counter;
    public string $uri_site;
    public int $visits;
    public int $views;
    public int $users;
}