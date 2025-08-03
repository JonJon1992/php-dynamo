<?php

declare(strict_types=1);

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
    protected DynamoDbClient $client;
    protected Marshaler $marshaler;
    protected DynamoConfig $config;

    public function __construct(?DynamoConfig $config = null)
    {
        $this->config = $config ?? new DynamoConfig();
        $this->marshaler = new Marshaler();
        $this->client = new DynamoDbClient($this->config->getClientParams());
    }

    /**
     * Lista todas as tabelas
     */
    public function listTables(): array
    {
        $result = $this->client->listTables();
        return ['tables' => $result['TableNames']];
    }

    /**
     * Descreve uma tabela
     */
    public function describeTable(string $tableName): Result|array
    {
        try {
            return $this->client->describeTable(['TableName' => $tableName]);
        } catch (DynamoDbException $e) {
            if ($e->getAwsErrorCode() === 'ResourceNotFoundException') {
                return ['error' => $e->getAwsErrorMessage()];
            }
            throw $e;
        }
    }

    /**
     * Cria uma tabela
     */
    public function createTable(array $tableSettings): Result|array
    {
        try {
            return $this->client->createTable($tableSettings);
        } catch (DynamoDbException $e) {
            if ($e->getAwsErrorCode() === 'ResourceInUseException') {
                return ['error' => $e->getAwsErrorMessage()];
            }
            throw $e;
        }
    }

    /**
     * Remove uma tabela
     */
    public function deleteTable(string $tableName): Result|array
    {
        try {
            return $this->client->deleteTable(['TableName' => $tableName]);
        } catch (DynamoDbException $e) {
            if ($e->getAwsErrorCode() === 'ResourceNotFoundException') {
                return ['error' => $e->getAwsErrorMessage()];
            }
            throw $e;
        }
    }

    /**
     * Insere um item
     */
    public function putItem(string $tableName, $item): Result
    {
        $data = is_string($item) ? $item : json_encode($item, JSON_UNESCAPED_SLASHES);
        
        try {
            $marshaledItem = $this->marshaler->marshalJson($data);
            
            // Verificar se PK e SK estão presentes
            if (!isset($marshaledItem['PK'])) {
                throw new Exception("PK não encontrado no item. Dados: $data");
            }
            
            // Mapear keys se necessário
            $marshaledItem = $this->mapKeysToTableSchema($tableName, $marshaledItem);
            
            $params = [
                'TableName' => $tableName,
                'Item' => $marshaledItem
            ];
            
            return $this->client->putItem($params);
        } catch (Exception $e) {
            throw new Exception("Erro no putItem: " . $e->getMessage() . " - Dados: $data");
        }
    }

    /**
     * Insere múltiplos itens
     */
    public function putItems(string $tableName, array $items): array
    {
        $results = [];
        foreach ($items as $item) {
            $results[] = $this->putItem($tableName, $item);
        }
        return $results;
    }

    /**
     * Busca um item
     */
    public function getItem(string $tableName, array $key): int|SetValue|array|BinaryValue|stdClass|NumberValue|null
    {
        $result = $this->client->getItem($this->prepareItem($tableName, json_encode($key)));
        return isset($result['Item']) ? $this->marshaler->unmarshalItem($result['Item']) : null;
    }

    /**
     * Atualiza um item
     */
    public function updateItem(
        string $tableName,
        array $key,
        string $updateExpression,
        array $values,
        ?string $condition = null
    ): Result {
        $params = [
            'TableName' => $tableName,
            'Key' => $this->marshaler->marshalJson(json_encode($key)),
            'UpdateExpression' => 'SET ' . $updateExpression,
            'ExpressionAttributeValues' => $this->marshaler->marshalJson(json_encode($values)),
            'ReturnValues' => 'UPDATED_NEW'
        ];

        if ($condition) {
            $params['ConditionExpression'] = $condition;
        }

        return $this->client->updateItem($params);
    }

    /**
     * Remove um item
     */
    public function deleteItem(string $tableName, array $key, ?string $condition = null, ?array $values = null): Result
    {
        $params = [
            'TableName' => $tableName,
            'Key' => $this->marshaler->marshalJson(json_encode($key))
        ];

        if ($condition) {
            $params['ConditionExpression'] = $condition;
        }

        if ($values) {
            $params['ExpressionAttributeValues'] = $this->marshaler->marshalJson(json_encode($values));
        }

        return $this->client->deleteItem($params);
    }

    /**
     * Executa uma query
     */
    public function query(
        string $tableName,
        string $keyConditionExpression,
        array $values,
        ?string $filterExpression = null,
        ?string $projectionExpression = null,
        ?array $expressionAttributeNames = null,
        ?array $params = []
    ): array {
        $queryParams = [
            'TableName' => $tableName,
            'KeyConditionExpression' => $keyConditionExpression,
            'ExpressionAttributeValues' => $this->marshaler->marshalJson(json_encode($values)),
            'Limit' => $params['Limit'] ?? 10,
        ];

        if ($filterExpression) {
            $queryParams['FilterExpression'] = $filterExpression;
        }

        if ($projectionExpression) {
            $queryParams['ProjectionExpression'] = $projectionExpression;
        }

        if ($expressionAttributeNames) {
            $queryParams['ExpressionAttributeNames'] = $expressionAttributeNames;
        }

        if (isset($params['ExclusiveStartKey'])) {
            $queryParams['ExclusiveStartKey'] = $this->marshaler->marshalJson(json_encode($params['ExclusiveStartKey']));
        }

        if (isset($params['IndexName'])) {
            $queryParams['IndexName'] = $params['IndexName'];
        }

        if (isset($params['ScanIndexForward'])) {
            $queryParams['ScanIndexForward'] = $params['ScanIndexForward'];
        }

        $result = $this->client->query($queryParams);
        
        $items = array_map(
            fn($item) => $this->marshaler->unmarshalItem($item),
            $result['Items']
        );

        $lastKey = null;
        if (isset($result['LastEvaluatedKey']) && !empty($result['LastEvaluatedKey'])) {
            $lastKey = $this->marshaler->unmarshalItem($result['LastEvaluatedKey']);
        }

        return [
            'items' => $items,
            'lastKey' => $lastKey
        ];
    }

    /**
     * Executa um scan
     */
    public function scan(
        string $tableName,
        ?string $filterExpression = null,
        ?array $values = null,
        ?array $params = []
    ): array {
        $scanParams = [
            'TableName' => $tableName,
            'Limit' => $params['Limit'] ?? 10,
        ];

        if ($filterExpression) {
            $scanParams['FilterExpression'] = $filterExpression;
        }

        if ($values) {
            $scanParams['ExpressionAttributeValues'] = $this->marshaler->marshalJson(json_encode($values));
        }

        $result = $this->client->scan($scanParams);

        $items = array_map(
            fn($item) => $this->marshaler->unmarshalItem($item),
            $result['Items']
        );

        return [
            'items' => $items,
            'lastKey' => $result['LastEvaluatedKey'] ?? null
        ];
    }

    /**
     * Insere múltiplos itens usando WriteRequestBatch
     */
    public function batchWriteItems(string $tableName, array $items, array $config = []): WriteRequestBatch
    {
        $config['table'] = $tableName;
        $writeRequest = new WriteRequestBatch($this->client, $config);
        
        foreach ($items as $item) {
            $data = is_string($item) ? $item : json_encode($item, JSON_UNESCAPED_SLASHES);
            $writeRequest->put($this->marshaler->marshalJson($data));
        }
        
        return $writeRequest;
    }

    /**
     * Remove múltiplos itens usando WriteRequestBatch
     */
    public function batchDeleteItems(string $tableName, array $keys, array $config = []): WriteRequestBatch
    {
        $config['table'] = $tableName;
        $writeRequest = new WriteRequestBatch($this->client, $config);
        
        foreach ($keys as $key) {
            $data = is_string($key) ? $key : json_encode($key, JSON_UNESCAPED_SLASHES);
            $writeRequest->delete($this->marshaler->marshalJson($data));
        }
        
        return $writeRequest;
    }

    /**
     * Prepara dados para operações
     */
    private function prepareItem(string $tableName, string $data): array
    {
        $marshaledItem = $this->marshaler->marshalJson($data);
        
        return [
            'TableName' => $tableName,
            'Key' => $marshaledItem
        ];
    }

    /**
     * Retorna o cliente DynamoDB
     */
    public function getClient(): DynamoDbClient
    {
        return $this->client;
    }

    /**
     * Retorna a configuração
     */
    public function getConfig(): DynamoConfig
    {
        return $this->config;
    }

    /**
     * Mapeia as chaves do item para o schema da tabela
     */
    private function mapKeysToTableSchema(string $tableName, array $marshaledItem): array
    {
        try {
            $tableInfo = $this->describeTable($tableName);
            if (!is_array($tableInfo) || !isset($tableInfo['Table'])) {
                return $marshaledItem;
            }

            $keySchema = $tableInfo['Table']['KeySchema'] ?? [];
            $requiredKeys = array_map(fn($key) => $key['AttributeName'], $keySchema);
            
            // Se a tabela usa PK/SK, não precisa mapear
            if (in_array('PK', $requiredKeys) && in_array('SK', $requiredKeys)) {
                return $marshaledItem;
            }
            
            // Mapear PK/SK para os nomes corretos da tabela
            $mappedItem = $marshaledItem;
            
            // Mapear PK para o primeiro key (partition key)
            if (isset($marshaledItem['PK']) && !empty($requiredKeys)) {
                $partitionKey = $requiredKeys[0];
                $mappedItem[$partitionKey] = $marshaledItem['PK'];
                unset($mappedItem['PK']);
            }
            
            // Mapear SK para o segundo key (sort key) se existir
            if (isset($marshaledItem['SK']) && count($requiredKeys) > 1) {
                $sortKey = $requiredKeys[1];
                $mappedItem[$sortKey] = $marshaledItem['SK'];
                unset($mappedItem['SK']);
            }
            
            return $mappedItem;
        } catch (Exception $e) {
            // Se não conseguir obter o schema, retorna o item original
            return $marshaledItem;
        }
    }

    /**
     * Verifica se uma tabela existe
     */
    public function tableExists(string $tableName): bool
    {
        try {
            $result = $this->describeTable($tableName);
            return !isset($result['error']);
        } catch (Exception $e) {
            return false;
        }
    }

    /**
     * Aguarda até que uma tabela esteja ativa
     */
    public function waitForTableActive(string $tableName, int $maxAttempts = 30): bool
    {
        for ($attempt = 0; $attempt < $maxAttempts; $attempt++) {
            try {
                $result = $this->describeTable($tableName);
                if (isset($result['Table']['TableStatus']) && $result['Table']['TableStatus'] === 'ACTIVE') {
                    return true;
                }
                sleep(2);
            } catch (Exception $e) {
                sleep(2);
            }
        }
        return false;
    }

    /**
     * Prepara dados para operação PUT
     */
    public function toPrepare(string $table, $doc): array
    {
        $data = is_string($doc) ? $doc : json_encode($doc, JSON_UNESCAPED_SLASHES);
        $marshaledItem = $this->marshaler->marshalJson($data);
        
        return [
            'TableName' => $table,
            'Item' => $marshaledItem
        ];
    }

    /**
     * Prepara dados para operação DELETE
     */
    public function toPrepareDelete(string $table, array $key, ?string $condition = null, ?array $values = null): array
    {
        $params = [
            'TableName' => $table,
            'Key' => $this->marshaler->marshalJson(json_encode($key))
        ];

        if ($condition) {
            $params['ConditionExpression'] = $condition;
        }

        if ($values) {
            $params['ExpressionAttributeValues'] = $this->marshaler->marshalJson(json_encode($values));
        }

        return $params;
    }

    /**
     * Prepara dados para operação UPDATE
     */
    public function toPrepareUpdate(string $table, string $updateExpression, array $values, array $key, ?string $condition = null): array
    {
        $params = [
            'TableName' => $table,
            'Key' => $this->marshaler->marshalJson(json_encode($key)),
            'UpdateExpression' => 'SET ' . $updateExpression,
            'ExpressionAttributeValues' => $this->marshaler->marshalJson(json_encode($values)),
            'ReturnValues' => 'UPDATED_NEW'
        ];

        if ($condition) {
            $params['ConditionExpression'] = $condition;
        }

        return $params;
    }
}