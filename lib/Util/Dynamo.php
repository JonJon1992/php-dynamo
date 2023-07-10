<?php

namespace Jonjon\PhpDynamo\Util;

use Aws\DynamoDb\BinaryValue;
use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\DynamoDb\NumberValue;
use Aws\DynamoDb\SetValue;
use Aws\DynamoDb\WriteRequestBatch;
use Aws\Result;
use Exception;
use stdClass;

class Dynamo
{
    protected DynamoDbClient $dynamo;

    public function __construct(
        string $key,
        string $secret,
        string $region,
        string $endpoint = null
    )
    {

        $params = [
            'credentials' => [
                'key' => $key,
                'secret' => $secret
            ],
            'region' => $region,
            'version' => 'latest'
        ];

        if ($endpoint) {
            $params['endpoint'] = $endpoint;
        }
        $this->dynamo = new DynamoDbClient($params);

    }

    public function list(): array
    {
        $receipt = $this->dynamo->listTables();
        $tables = $receipt['TableNames'];

        return ['tables' => $tables];
    }

    public function describe(string $table): Result|DynamoDbException|Exception|array
    {
        try {

            $receipt = $this->dynamo->describeTable(['TableName' => $table]);
        } catch (DynamoDbException $exception) {
            if ($exception->getAwsErrorCode() == 'ResourceNotFoundException') {
                return [$exception->getAwsErrorMessage()];
            }
            return $exception;
        }

        return $receipt;
    }

    public function create(array $tableSettings): Result|DynamoDbException|Exception|array
    {
        try {
            $receipt = $this->dynamo->createTable($tableSettings);
        } catch (DynamoDbException $exception) {
            if ($exception->getAwsErrorCode() == 'ResourceInUseException') {
                return [$exception->getAwsErrorMessage()];
            }
            return $exception;
        }

        return $receipt;
    }

    public function drop(string $table): Result|DynamoDbException|Exception|array
    {
        try {
            $receipt = $this->dynamo->deleteTable(['TableName' => $table]);
        } catch (DynamoDbException $exception) {
            if ($exception->getAwsErrorCode() == 'ResourceInUseException') {
                return [$exception->getAwsErrorMessage()];
            }
            return $exception;
        }
        return $receipt;
    }

    public function putAll(
        string $table,
        array  $data = []
    ): array
    {
        $receipt = [];
        foreach ($data as $item) {
            $receipt[] = $this->dynamo->putItem($this->toPrepare($table, json_encode($item)));
        }
        return $receipt;
    }

    public function put(
        string $table,
               $value
    ): Result
    {

        return $this->dynamo->putItem($this->toPrepare($table,
            is_string($value) ? $value : json_encode($value, JSON_UNESCAPED_SLASHES)));
    }

    public function get(
        string $table,
        array  $key
    ): int|SetValue|array|BinaryValue|stdClass|NumberValue|null
    {
        $marshal = new Marshaler();
        $result = $this->dynamo->getItem($this->toPrepare($table, json_encode($key)));
        return isset($result['Item']) ? $marshal->unmarshalItem($result['Item']) : null;
    }

    public function update(
        string $table,
        string $updateExpression,
        array  $values,
        array  $key,
        string $condition = null
    ): Result
    {
        return $this->dynamo->updateItem($this->toPrepareUpdate($table, $updateExpression, $values, $key, $condition));
    }

    public function delete(
        string $table,
        array  $key,
        string $condition = null,
        array  $values = null
    ): Result
    {
        return $this->dynamo->deleteItem($this->toPrepareDelete($table, $key, $condition, $values));
    }

    public function query(
        string $table,
        string $keyCondition,
        array  $values,
        string $filter = null,
        string $attributes = null,
        array  $names = null,
        array  $params = null
    ): array
    {

        $marshal = new Marshaler();

        $receipt = $this->dynamo->query($this->toPrepareQuery(
            $table,
            $keyCondition,
            $values,
            $filter,
            $attributes,
            $names,
            $params
        ));
        $items = [];

        foreach ($receipt['Items'] as $item) {
            $items = $marshal->unmarshalItem($item, true);
        }
        $lastKey = null;
        if ($receipt['LastEvaluatedKey']) {
            $lastKey = $marshal->unmarshalItem($receipt['LastEvaluatedKey']);
        }

        return [
            'items' => $items,
            'lastKey' => $lastKey
        ];
    }

    public function toPrepare(
        $table,
        $serializeItem
    ): array
    {
        $marshal = new Marshaler();
        return [
            'TableName' => $table,
            'Item' => $marshal->marshalJson($serializeItem)];
    }

    public function toPrepareUpdate(
        string $table,
        string $updateExpression,
        array  $values,
        array  $key,
        string $condition = null
    ): array
    {
        $marshal = new Marshaler();
        $params = [
            'TableName' => $table,
            'Key' => $marshal->marshalJson(json_encode($key)),
            'UpdateExpression' => 'set ' . $updateExpression,
            'ExpressionAttributeValues' => $marshal->marshalJson(json_encode($values)),
            'ReturnValues' => 'UPDATED_NEW'
        ];
        if (!empty($condition)) {
            $params['ConditionExpression'] = $condition;
        }
        return $params;
    }

    public function toPrepareDelete(
        string $table,
        array  $key,
        string $condition = null,
        array  $values = null
    ): array
    {
        $marshal = new Marshaler();
        $params = [
            'TableName' => $table,
            'Key' => $marshal->marshalJson(json_encode($key))
        ];
        if (!empty($condition)) {
            $params['ConditionExpression'] = $condition;
        }
        if (!empty($values)) {
            $params['ExpressionAttributeValues'] = $marshal->marshalJson(json_encode($values));
        }
        return $params;
    }

    public function toPrepareQuery(
        string $table,
        string $keyCondition,
        array  $values,
        string $filter = null,
        string $attributes = null,
        array  $names = null,
        array  $params = null
    ): array
    {
        $marshal = new Marshaler();
        $params = [
            'TableName' => $table,
            'KeyConditionExpression' => $keyCondition,
            'ExpressionAttributeValues' => $marshal->marshalJson(json_encode($values)),
            'Limit' => $params['Limit'] ?? 10,
        ];
        if (!empty($filter)) {
            $params['FilterExpression'] = $filter;
        }
        if (!empty($attributes)) {
            $params['ProjectionExpression'] = $attributes;
        }
        if (!empty($names)) {
            $params['ExpressionAttributeValues'] = $names;
        }
        if (isset($params['ExclusiveStartKey'])) {
            $params['ExclusiveStartKey'] = $marshal->marshalJson(json_encode($params['ExclusiveStartKey']));
        }

        return $params;
    }

    public function scan(
        string $table,
        string $filterExpression = null,
        array  $values = null,
        array  $params = null
    ): array
    {
        $marshal = new Marshaler();

        $params['TableName'] = $table;
        $params['Limit'] = $params['Limit'] ?? 10;

        if (!empty($filterExpression)) {
            $params['FilterExpression'] = $filterExpression;
        }
        if (!empty($values)) {
            $params['ExpressionAttributeValues'] = $marshal->marshalJson(json_encode($values));
        }
        $receipt = $this->dynamo->scan($params);

        $items = [];

        foreach ($receipt['Items'] as $item) {
            $items[] = $marshal->unmarshalItem($item);
        }

        return [
            'items' => $items,
            'lastKey' => $receipt['LastEvaluatedKey']
        ];
    }

    public function insertItems(
        string $table,
        array  $itens,
        array  $config = []
    ): WriteRequestBatch
    {
        $marshal = new Marshaler();
        $config['table'] = $table;

        $writeRequest = new WriteRequestBatch($this->dynamo, $config);
        array_map(function ($item) use ($writeRequest, $marshal) {
            $writeRequest->put($marshal->marshalJson(is_string($item) ? $item
                : json_encode($item, JSON_UNESCAPED_SLASHES)));
        }, $itens);
        return $writeRequest;
    }

    public function deleteItems(
        string $table,
        array  $keys,
        array  $config = []
    ): WriteRequestBatch
    {
        $marshal = new Marshaler();
        $config['table'] = $table;

        $writeRequest = new WriteRequestBatch($this->dynamo, $config);
        array_map(function ($key) use ($writeRequest, $marshal) {
            $writeRequest->put($marshal->marshalJson(is_string($key) ?
                $key : json_encode($key, JSON_UNESCAPED_SLASHES)));
        }, $keys);
        return $writeRequest;
    }

    public function getClient(): DynamoDbClient
    {
        return $this->dynamo;
    }
}