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
use RuntimeException;

class DynamoManager
{
    public const HYDRATE_OBJECT = 'object';
    public const HYDRATE_ARRAY = 'array';
    public const HYDRATE_STDCLASS = 'stdclass';

    private function __construct(
        protected Dynamo $dynamo,
        protected DynamoTransaction $transactions,
        protected ?string $namespace = null,
        protected ?string $itemsNamespace = null
    ) {
    }

    /**
     * Cria uma nova instância do DynamoManager
     */
    public static function create(?DynamoConfig $config = null, ?string $namespace = null): self
    {
        $dynamo = new Dynamo($config);
        $transactions = new DynamoTransaction($dynamo);
        return new self($dynamo, $transactions, $namespace, $namespace);
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
        return self::create($config, $namespace);
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
    public function find(string $tableName, array $key, ?string $class = null): ?DynamoItem
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
        ?string $class = null,
        string $hydrate = self::HYDRATE_OBJECT
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

        $result['items'] = $this->convertItems($result['items'], $hydrate, $class);
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
        ?string $class = null,
        string $hydrate = self::HYDRATE_OBJECT
    ): array {
        $result = $this->dynamo->scan($tableName, $filterExpression, $values, $params);
        $result['items'] = $this->convertItems($result['items'], $hydrate, $class);
        return $result;
    }

    /**
     * Insere múltiplos itens em lote. Os itens são enviados antes do retorno.
     *
     * @param DynamoItem[] $items
     * @param array<string, mixed> $config Opções do WriteRequestBatch
     */
    public function insertItems(array $items, array $config = []): ?WriteRequestBatch
    {
        if ($items === []) {
            return null;
        }

        return $this->dynamo->batchWriteItems(
            $this->resolveBatchTable($items),
            array_map(fn(DynamoItem $item) => $item->serialize(), $items),
            $config
        );
    }

    /**
     * Remove múltiplos itens em lote. As remoções são enviadas antes do retorno.
     *
     * @param DynamoItem[] $items
     * @param array<string, mixed> $config Opções do WriteRequestBatch
     */
    public function deleteItems(array $items, array $config = []): ?WriteRequestBatch
    {
        if ($items === []) {
            return null;
        }

        return $this->dynamo->batchDeleteItems(
            $this->resolveBatchTable($items),
            array_map(fn(DynamoItem $item) => $item->getKey(), $items),
            $config
        );
    }

    /**
     * Cria um query builder para a classe de item informada
     *
     * @param class-string<DynamoItem> $class
     */
    public function getQueryBuilder(string $class): DynamoQueryBuilder
    {
        return new DynamoQueryBuilder($this, $class);
    }

    /**
     * Converte um item para o formato especificado
     */
    public function convert(mixed $item, string $hydrate = self::HYDRATE_OBJECT, ?string $class = null): mixed
    {
        if (empty($item)) {
            return null;
        }

        return match ($hydrate) {
            self::HYDRATE_OBJECT => $this->convertToObject($item, $class),
            self::HYDRATE_ARRAY => json_decode(
                json_encode($item, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
                true,
                flags: JSON_THROW_ON_ERROR
            ),
            self::HYDRATE_STDCLASS => (object) $item,
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
    public function itemToDynamoItem(mixed $item, string $class): DynamoItem
    {
        if (!is_a($class, DynamoItem::class, true)) {
            throw new RuntimeException(
                "Erro ao criar instância da classe $class: a classe não existe ou não estende DynamoItem"
            );
        }

        return new $class($item);
    }

    /**
     * Determina a classe do item baseado no item ou namespace
     */
    private function getItemClassByItem(mixed $item, ?string $class): string
    {
        $item = (object) $item;

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
    private function convertToObject(mixed $item, ?string $class): DynamoItem
    {
        $class = $this->getItemClassByItem($item, $class);
        return $this->itemToDynamoItem($item, $class);
    }

    /**
     * Atualiza um item
     *
     * A expressão pode começar com SET, REMOVE, ADD ou DELETE; sem cláusula, assume SET.
     *
     * @param array<string, string>|null $names ExpressionAttributeNames (ex.: ['#n' => 'name'])
     */
    public function update(
        string $tableName,
        array $key,
        string $updateExpression,
        array $values,
        ?string $condition = null,
        ?array $names = null
    ): Result {
        return $this->dynamo->updateItem($tableName, $key, $updateExpression, $values, $condition, $names);
    }

    /**
     * Garante que todos são DynamoItem da mesma tabela e retorna o nome dela
     */
    private function resolveBatchTable(array $items): string
    {
        $tableName = null;

        foreach ($items as $item) {
            if (!$item instanceof DynamoItem) {
                throw new InvalidArgumentException("Todos os itens devem ser instâncias de DynamoItem");
            }

            $tableName ??= $item::getTableName();

            if ($item::getTableName() !== $tableName) {
                throw new InvalidArgumentException("Items de tabelas diferentes não são permitidos");
            }
        }

        return $tableName ?? throw new InvalidArgumentException("Nenhum item informado");
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
