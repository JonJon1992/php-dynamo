<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo;

use Aws\DynamoDb\WriteRequestBatch;
use Aws\Result;
use InvalidArgumentException;
use Jonjon\PhpDynamo\Item\DynamoItem;
use Jonjon\PhpDynamo\Util\Dynamo;
use Jonjon\PhpDynamo\Util\DynamoConfig;
use Jonjon\PhpDynamo\Util\DynamoTransaction;
use ReflectionClass;
use ReflectionException;
use RuntimeException;

class DynamoManager
{
    protected Dynamo $dynamo;
    protected ?string $namespace;
    protected ?string $itemsNamespace;
    protected DynamoTransaction $transactions;

    public const HYDRATE_OBJECT = 'object';
    public const HYDRATE_ARRAY = 'array';
    public const HYDRATE_STDCLASS = 'stdclass';

    private function __construct(?DynamoConfig $config = null, ?string $namespace = null)
    {
        $this->dynamo = new Dynamo($config);
        $this->namespace = $namespace;
        $this->itemsNamespace = $namespace;
        $this->transactions = new DynamoTransaction($this->dynamo);
    }

    /**
     * Cria uma nova instância do DynamoManager
     */
    public static function create(?DynamoConfig $config = null, ?string $namespace = null): self
    {
        return new self($config, $namespace);
    }

    /**
     * Cria uma instância com configuração manual (legacy)
     */
    public static function createWithCredentials(
        string $key,
        string $secret,
        string $region,
        ?string $endpoint = null,
        ?string $namespace = null
    ): self {
        $config = new DynamoConfig($key, $secret, $region, $endpoint);
        return new self($config, $namespace);
    }

    /**
     * Salva um item
     */
    public function save(DynamoItem $item): Result
    {
        return $this->dynamo->putItem($item::getTableName(), $item->serialize());
    }

    /**
     * Remove um item
     */
    public function delete(DynamoItem $item): Result
    {
        return $this->dynamo->deleteItem($item::getTableName(), $item->getKey());
    }

    /**
     * Busca um item
     */
    public function find(string $tableName, array $key, ?string $class = null)
    {
        $item = $this->dynamo->getItem($tableName, $key);
        return $this->convert($item, self::HYDRATE_OBJECT, $class);
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
        ?array $params = [],
        ?string $class = null
    ): array {
        $result = $this->dynamo->query(
            $tableName,
            $keyConditionExpression,
            $values,
            $filterExpression,
            $projectionExpression,
            $expressionAttributeNames,
            $params
        );

        $result['items'] = $this->convertItems($result['items'], self::HYDRATE_OBJECT, $class);
        return $result;
    }

    /**
     * Executa um scan
     */
    public function scan(
        string $tableName,
        ?string $filterExpression = null,
        ?array $values = null,
        ?array $params = [],
        ?string $class = null
    ): array {
        $result = $this->dynamo->scan($tableName, $filterExpression, $values, $params);
        $result['items'] = $this->convertItems($result['items'], self::HYDRATE_OBJECT, $class);
        return $result;
    }

    /**
     * Insere múltiplos itens
     */
    public function insertItems(array $items, array $config = []): ?WriteRequestBatch
    {
        if (empty($items)) {
            return null;
        }

        $tableName = $items[0]::getTableName();
        
        foreach ($items as $item) {
            if (!$item instanceof DynamoItem) {
                throw new InvalidArgumentException("Todos os itens devem ser instâncias de DynamoItem");
            }
            if ($item::getTableName() !== $tableName) {
                throw new InvalidArgumentException("Items de tabelas diferentes não são permitidos");
            }
        }

        return $this->dynamo->batchWriteItems(
            $tableName,
            array_map(fn(DynamoItem $item) => $item->serialize(), $items),
            $config
        );
    }

    /**
     * Remove múltiplos itens
     */
    public function deleteItems(array $items, array $config = []): ?WriteRequestBatch
    {
        if (empty($items)) {
            return null;
        }

        $tableName = $items[0]::getTableName();
        
        foreach ($items as $item) {
            if (!$item instanceof DynamoItem) {
                throw new InvalidArgumentException("Todos os itens devem ser instâncias de DynamoItem");
            }
            if ($item::getTableName() !== $tableName) {
                throw new InvalidArgumentException("Items de tabelas diferentes não são permitidos");
            }
        }

        return $this->dynamo->batchDeleteItems(
            $tableName,
            array_map(fn(DynamoItem $item) => $item->getKey(), $items),
            $config
        );
    }

    /**
     * Converte um item para o formato especificado
     */
    public function convert($item, string $hydrate = self::HYDRATE_OBJECT, ?string $class = null)
    {
        if (empty($item)) {
            return null;
        }

        return match ($hydrate) {
            self::HYDRATE_OBJECT => $this->convertToObject($item, $class),
            self::HYDRATE_ARRAY => json_decode(json_encode($item, JSON_UNESCAPED_SLASHES), true),
            self::HYDRATE_STDCLASS => (object)$item,
            default => $item,
        };
    }

    /**
     * Converte múltiplos itens
     */
    public function convertItems(array $items, string $hydrate = self::HYDRATE_OBJECT, ?string $class = null): array
    {
        return array_map(fn($item) => $this->convert($item, $hydrate, $class), $items);
    }

    /**
     * Retorna a instância do Dynamo
     */
    public function getDynamo(): Dynamo
    {
        return $this->dynamo;
    }

    /**
     * Retorna a instância de transações
     */
    public function getTransaction(): DynamoTransaction
    {
        return $this->transactions;
    }

    /**
     * Converte um item para DynamoItem
     */
    public function itemToDynamoItem($item, string $class): DynamoItem
    {
        try {
            $reflect = new ReflectionClass($class);
            return $reflect->newInstance($item);
        } catch (ReflectionException $e) {
            throw new RuntimeException("Erro ao criar instância da classe $class: " . $e->getMessage());
        }
    }

    /**
     * Determina a classe do item baseado no item ou namespace
     */
    private function getItemClassByItem($item, ?string $class): string
    {
        $item = (object)$item;

        if (!empty($item->ItemType) && !empty($this->itemsNamespace)) {
            return "$this->itemsNamespace\\$item->ItemType";
        }

        if (empty($class)) {
            throw new InvalidArgumentException("Tipo do item não encontrado");
        }

        return $class;
    }

    /**
     * Converte item para objeto
     */
    private function convertToObject($item, ?string $class)
    {
        $class = $this->getItemClassByItem($item, $class);
        return $this->itemToDynamoItem($item, $class);
    }

    /**
     * Atualiza um item
     */
    public function update(
        string $tableName,
        array $key,
        string $updateExpression,
        array $values,
        ?string $condition = null
    ): Result {
        return $this->dynamo->updateItem($tableName, $key, $updateExpression, $values, $condition);
    }

    /**
     * Lista todas as tabelas
     */
    public function listTables(): array
    {
        return $this->dynamo->listTables();
    }

    /**
     * Descreve uma tabela
     */
    public function describeTable(string $tableName): Result|array
    {
        return $this->dynamo->describeTable($tableName);
    }

    /**
     * Cria uma tabela
     */
    public function createTable(array $tableSettings): Result|array
    {
        return $this->dynamo->createTable($tableSettings);
    }

    /**
     * Remove uma tabela
     */
    public function deleteTable(string $tableName): Result|array
    {
        return $this->dynamo->deleteTable($tableName);
    }
}