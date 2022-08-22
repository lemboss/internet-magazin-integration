<?php

namespace app\Components\GoogleSheets;

use Google_Client;
use Google_Service_Sheets;
use Google_Service_Sheets_ValueRange;

class GoogleSheetsIntegration
{
    protected $googleAccountKeyFilePath;
    protected $service;

    public function __construct()
    {
        // path to account service keys 
        $this->googleAccountKeyFilePath = dirname(__DIR__, 3) . '/bg-keys.json';

        $client = new Google_Client();
        putenv('GOOGLE_APPLICATION_CREDENTIALS=' . $this->googleAccountKeyFilePath);
        $client->useApplicationDefaultCredentials();
        $client->addScope('https://www.googleapis.com/auth/spreadsheets');
        $this->service = new Google_Service_Sheets($client);
    }

    public function addRowsToEnd(string $spreadsheetId, string $listTitle, array $rows)
    {
        $rowValues = $this->getRows($spreadsheetId, $listTitle);
        $body = new Google_Service_Sheets_ValueRange(['values' => $rows]);
        $options = ['valueInputOption' => 'USER_ENTERED'];
        $length = $this->findFirstNonEmptyLine($rowValues);
        $range = $listTitle . '!A' . ($length + 1);
        $this->service->spreadsheets_values->update($spreadsheetId, $range, $body, $options);
    }

    public function addRowsFrom(string $spreadsheetId, string $listTitle, array $rows, int $startIndex)
    {
        $body = new Google_Service_Sheets_ValueRange(['values' => $rows]);
        $options = ['valueInputOption' => 'USER_ENTERED'];
        $range = $listTitle . '!A' . $startIndex;
        $this->service->spreadsheets_values->update($spreadsheetId, $range, $body, $options);
    }

    public function getRows(string $spreadsheetId, string $listTitle): array
    {
        $response = $this->service->spreadsheets_values->get($spreadsheetId, $listTitle, ['valueRenderOption' => 'FORMULA']);
        return $response->getValues();
    }

    public function findFirstNonEmptyLine(array $rows): int
    {
        for ($i = count($rows) - 1; $i > 0; $i--) {
            $value = $rows[$i][0] ?? '';
            if ($value !== '') {
                return $i + 1;
            }
        }

        return 1;
    }

    public function findRowByValue(array $rows, int $index, $value): ?int
    {
        foreach ($rows as $key => $row) {
            $rowValue = $row[$index] ?? '-1';
            if ($rowValue == $value) {
                return $key;
            }
        }

        return null;
    }

    public function findIndexByValue(array $rows, $value): ?int
    {
        $index = 1;
        foreach ($rows as $key => $row)
        {
            if (in_array($value, $row))
                return $index;
            ++$index;
        }

        return null;
    }

    public function updateRowByValue(string $spreadsheetId, string $listTitle, array $rows, $value)
    {
        $rowValues = $this->getRows($spreadsheetId, $listTitle);
        $index = $this->findIndexByValue($rowValues, $value);
        $this->addRowsFrom($spreadsheetId, $listTitle, $rows, $index);
    }
}