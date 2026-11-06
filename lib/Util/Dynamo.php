<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Util;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\DynamoDb\WriteRequestBatch;
use Aws\Exception\AwsException;
use Aws\Result;
use Exception;
use InvalidArgumentException;
use RuntimeException;

class Dynamo
{
    protected DynamoDbClient $client;
    protected Marshaler $marshaler;
    protected DynamoConfig $config;

    /**
     * Por tabela: nomes reais das chaves quando diferem de PK/SK (ex.: ['PK' => 'id']).
     *
     * @var array<string, array<string, string>>
     */
    protected array $tableKeySchemaCache = [];

    public function __construct(?DynamoConfig $config = null)
    {
        $this->config = $config ?? DynamoConfig::fromEnvironment();
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
     *
     * Erros do DynamoDB são propagados como DynamoDbException, sem alteração.
     */
    public function putItem(string $tableName, array|object|string $item): Result
    {
        $marshaledItem = $this->marshalData($item);
        if (!isset($marshaledItem['PK'])) {
            throw new InvalidArgumentException("O item para a tabela '$tableName' não possui o atributo 'PK'.");
        }

        return $this->client->putItem([
            'TableName' => $tableName,
            'Item' => $this->mapKeysToTableSchema($tableName, $marshaledItem),
        ]);
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
    public function getItem(string $tableName, array $key): ?array
    {
        $result = $this->client->getItem([
            'TableName' => $tableName,
            'Key' => $this->marshalKey($tableName, $key),
        ]);

        return isset($result['Item']) ? $this->unmarshal($result['Item']) : null;
    }

    /**
     * Atualiza um item
     *
     * A expressão pode começar com SET, REMOVE, ADD ou DELETE; sem cláusula, assume SET.
     *
     * @param array<string, string>|null $names ExpressionAttributeNames (ex.: ['#n' => 'name'])
     */
    public function updateItem(
        string $tableName,
        array $key,
        string $updateExpression,
        array $values,
        ?string $condition = null,
        ?array $names = null
    ): Result {
        $params = $this->buildUpdateParams($tableName, $key, $updateExpression, $values, $condition, $names);
        $params['ReturnValues'] = 'UPDATED_NEW';

        return $this->client->updateItem($params);
    }

    /**
     * Remove um item
     *
     * @param array<string, string>|null $names ExpressionAttributeNames usados na condição
     */
    public function deleteItem(
        string $tableName,
        array $key,
        ?string $condition = null,
        ?array $values = null,
        ?array $names = null
    ): Result {
        return $this->client->deleteItem($this->buildDeleteParams($tableName, $key, $condition, $values, $names));
    }

    public function query(
        string $tableName,
        string $keyConditionExpression,
        array $values,
        ?string $filterExpression = null,
        ?string $projectionExpression = null,
        ?array $expressionAttributeNames = null,
        ?array $params = []
    ): array {
        $params ??= [];

        $queryParams = [
            'TableName' => $tableName,
            'KeyConditionExpression' => $keyConditionExpression,
            'ExpressionAttributeValues' => $this->marshalData($values),
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

        $queryParams = $this->applyOperationParams(
            $queryParams,
            $params,
            ['Limit', 'IndexName', 'ScanIndexForward', 'Select']
        );

        $result = $this->client->query($queryParams);

        $items = array_map(
            fn($item) => $this->unmarshal($item),
            $result['Items'] ?? []
        );

        return [
            'items' => $items,
            'lastKey' => $this->unmarshalLastKey($result),
            'Count' => $result['Count'] ?? count($items),
            'ScannedCount' => $result['ScannedCount'] ?? null
        ];
    }

    public function scan(
        string $tableName,
        ?string $filterExpression = null,
        ?array $values = null,
        ?array $params = []
    ): array {
        $params ??= [];

        $scanParams = [
            'TableName' => $tableName,
        ];

        if ($filterExpression) {
            $scanParams['FilterExpression'] = $filterExpression;
        }

        if ($values) {
            $scanParams['ExpressionAttributeValues'] = $this->marshalData($values);
        }

        $scanParams = $this->applyOperationParams(
            $scanParams,
            $params,
            ['Limit', 'IndexName', 'ProjectionExpression', 'ExpressionAttributeNames', 'Select']
        );

        $result = $this->client->scan($scanParams);

        $items = array_map(
            fn($item) => $this->unmarshal($item),
            $result['Items'] ?? []
        );

        return [
            'items' => $items,
            'lastKey' => $this->unmarshalLastKey($result),
            'Count' => $result['Count'] ?? count($items),
            'ScannedCount' => $result['ScannedCount'] ?? null
        ];
    }

    /**
     * @param array<string, mixed> $config Opções do WriteRequestBatch (batch_size, pool_size, before, error)
     */
    public function batchWriteItems(string $tableName, array $items, array $config = []): WriteRequestBatch
    {
        return $this->runWriteBatch($tableName, $config, function (WriteRequestBatch $batch) use ($tableName, $items) {
            foreach ($items as $item) {
                $batch->put($this->mapKeysToTableSchema($tableName, $this->marshalData($item)));
            }
        });
    }

    /**
     * @param array<string, mixed> $config Opções do WriteRequestBatch (batch_size, pool_size, before, error)
     */
    public function batchDeleteItems(string $tableName, array $keys, array $config = []): WriteRequestBatch
    {
        return $this->runWriteBatch($tableName, $config, function (WriteRequestBatch $batch) use ($tableName, $keys) {
            foreach ($keys as $key) {
                $batch->delete($this->mapKeysToTableSchema($tableName, $this->marshalData($key)));
            }
        });
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
    public function toPrepare(string $table, array|object|string $doc): array
    {
        return [
            'TableName' => $table,
            'Item' => $this->mapKeysToTableSchema($table, $this->marshalData($doc)),
        ];
    }

    /**
     * Prepara dados para operação DELETE
     *
     * @param array<string, string>|null $names
     */
    public function toPrepareDelete(
        string $table,
        array $key,
        ?string $condition = null,
        ?array $values = null,
        ?array $names = null
    ): array {
        return $this->buildDeleteParams($table, $key, $condition, $values, $names);
    }

    /**
     * Prepara dados para operação UPDATE
     *
     * @param array<string, string>|null $names
     */
    public function toPrepareUpdate(
        string $table,
        string $updateExpression,
        array $values,
        array $key,
        ?string $condition = null,
        ?array $names = null
    ): array {
        return $this->buildUpdateParams($table, $key, $updateExpression, $values, $condition, $names);
    }

    /**
     * @param callable(WriteRequestBatch): void $enqueue
     * @param array<string, mixed> $config
     */
    private function runWriteBatch(string $tableName, array $config, callable $enqueue): WriteRequestBatch
    {
        $errors = [];
        $collectErrors = !isset($config['error']);

        $config['table'] = $tableName;
        if ($collectErrors) {
            $config['error'] = function (AwsException $error) use (&$errors): void {
                $errors[] = $error;
            };
        }

        $batch = new WriteRequestBatch($this->client, $config);
        $enqueue($batch);
        $batch->flush();

        if ($errors !== []) {
            throw new RuntimeException(
                sprintf(
                    'Falha em %d requisição(ões) de lote na tabela "%s": %s',
                    count($errors),
                    $tableName,
                    $errors[0]->getAwsErrorMessage() ?? $errors[0]->getMessage()
                ),
                0,
                $errors[0]
            );
        }

        return $batch;
    }

    private function buildUpdateParams(
        string $table,
        array $key,
        string $updateExpression,
        array $values,
        ?string $condition,
        ?array $names
    ): array {
        $params = [
            'TableName' => $table,
            'Key' => $this->marshalKey($table, $key),
            'UpdateExpression' => $this->normalizeUpdateExpression($updateExpression),
        ];

        if ($values !== []) {
            $params['ExpressionAttributeValues'] = $this->marshalData($values);
        }

        if ($names) {
            $params['ExpressionAttributeNames'] = $names;
        }

        if ($condition) {
            $params['ConditionExpression'] = $condition;
        }

        return $params;
    }

    private function buildDeleteParams(
        string $table,
        array $key,
        ?string $condition,
        ?array $values,
        ?array $names
    ): array {
        $params = [
            'TableName' => $table,
            'Key' => $this->marshalKey($table, $key),
        ];

        if ($condition) {
            $params['ConditionExpression'] = $condition;
        }

        if ($values) {
            $params['ExpressionAttributeValues'] = $this->marshalData($values);
        }

        if ($names) {
            $params['ExpressionAttributeNames'] = $names;
        }

        return $params;
    }

    private function normalizeUpdateExpression(string $expression): string
    {
        $expression = trim($expression);

        return preg_match('/^(SET|REMOVE|ADD|DELETE)\s/i', $expression) ? $expression : 'SET ' . $expression;
    }

    private function marshalKey(string $tableName, array $key): array
    {
        return $this->mapKeysToTableSchema($tableName, $this->marshalData($key));
    }

    private function marshalData(array|object|string $data): array
    {
        $json = is_string($data) ? $data : json_encode($data, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);

        return $this->marshaler->marshalJson($json);
    }

    private function mapKeysToTableSchema(string $tableName, array $attributes): array
    {
        foreach ($this->getTableKeyNames($tableName) as $logicalName => $tableKeyName) {
            if (!array_key_exists($logicalName, $attributes)) {
                continue;
            }

            $attributes[$tableKeyName] = $attributes[$logicalName];
            unset($attributes[$logicalName]);
        }

        return $attributes;
    }

    /**
     * @return array<string, string> Ex.: ['PK' => 'id', 'SK' => 'createdAt']; vazio se a tabela já usa PK/SK.
     */
    private function getTableKeyNames(string $tableName): array
    {
        if (array_key_exists($tableName, $this->tableKeySchemaCache)) {
            return $this->tableKeySchemaCache[$tableName];
        }

        try {
            $description = $this->describeTable($tableName);
        } catch (DynamoDbException $e) {
            return $this->tableKeySchemaCache[$tableName] = [];
        }

        if (!isset($description['Table'])) {
            return [];
        }

        $names = [];
        foreach ($description['Table']['KeySchema'] ?? [] as $keyDefinition) {
            $logicalName = $keyDefinition['KeyType'] === 'HASH' ? 'PK' : 'SK';
            if ($keyDefinition['AttributeName'] !== $logicalName) {
                $names[$logicalName] = $keyDefinition['AttributeName'];
            }
        }

        return $this->tableKeySchemaCache[$tableName] = $names;
    }

    private function applyOperationParams(array $operationParams, array $params, array $passthroughKeys): array
    {
        if (isset($params['ExclusiveStartKey'])) {
            $operationParams['ExclusiveStartKey'] = $this->marshalData($params['ExclusiveStartKey']);
        }

        foreach ($passthroughKeys as $key) {
            if (isset($params[$key])) {
                $operationParams[$key] = $params[$key];
            }
        }

        return $operationParams;
    }

    private function unmarshalLastKey(Result $result): ?array
    {
        if (!isset($result['LastEvaluatedKey']) || empty($result['LastEvaluatedKey'])) {
            return null;
        }

        return $this->unmarshal($result['LastEvaluatedKey']);
    }

    /**
     * @return array<string, mixed>
     */
    private function unmarshal(array $item): array
    {
        return (array) $this->marshaler->unmarshalItem($item);
    }
}
