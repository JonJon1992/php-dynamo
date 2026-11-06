<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Repository;

use Aws\Result;
use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\DynamoQueryBuilder;
use Jonjon\PhpDynamo\Item\DynamoItem;

abstract class DynamoRepository
{
    private DynamoManager $manager;
    private string $table;
    /** @var class-string<DynamoItem> */
    private string $itemClass;

    /**
     * @param class-string<DynamoItem> $itemClass
     */
    public function __construct(string $itemClass, DynamoManager $manager)
    {
        $this->manager = $manager;
        $this->itemClass = $itemClass;
        $this->table = $itemClass::getTableName();
    }

    /**
     * Busca um item pela chave primária e opcionalmente pela chave de ordenação
     */
    public function get(
        string $pk,
        ?string $sk = null,
        string $hydrate = DynamoManager::HYDRATE_OBJECT
    ): mixed {
        $key = ['PK' => $pk];
        if ($sk) {
            $key['SK'] = $sk;
        }

        $item = $this->manager->getDynamo()->getItem($this->table, $key);
        return $this->manager->convert($item, $hydrate, $this->itemClass);
    }

    /**
     * Executa uma query
     */
    public function query(
        string $keyCondition,
        array $values,
        ?string $filterExpression = null,
        ?string $attributes = null,
        ?array $names = null,
        ?array $params = null,
        string $hydrate = DynamoManager::HYDRATE_OBJECT
    ): array {
        $result = $this->manager->query(
            $this->table,
            $keyCondition,
            $values,
            $filterExpression,
            $attributes,
            $names,
            $params,
            $this->itemClass,
            $hydrate
        );

        return $result;
    }

    /**
     * Executa um scan
     */
    public function scan(
        ?string $filterExpression = null,
        ?array $values = null,
        ?array $params = null,
        string $hydrate = DynamoManager::HYDRATE_OBJECT
    ): array {
        $result = $this->manager->scan(
            $this->table,
            $filterExpression,
            $values,
            $params,
            $this->itemClass,
            $hydrate
        );

        return $result;
    }

    /**
     * Salva um item
     */
    public function save(DynamoItem $item): Result
    {
        return $this->manager->save($item);
    }

    /**
     * Remove um item
     */
    public function delete(DynamoItem $item): Result
    {
        return $this->manager->delete($item);
    }

    /**
     * Busca um item pela chave
     */
    public function findByKey(array $key, string $hydrate = DynamoManager::HYDRATE_OBJECT): mixed
    {
        $item = $this->manager->getDynamo()->getItem($this->table, $key);
        return $this->manager->convert($item, $hydrate, $this->itemClass);
    }

    /**
     * Busca todos os itens (usando scan)
     */
    public function findAll(array $params = [], string $hydrate = DynamoManager::HYDRATE_OBJECT): array
    {
        return $this->scan(null, null, $params, $hydrate);
    }

    /**
     * Busca o primeiro item que corresponde aos critérios
     */
    public function findFirst(
        string $keyCondition,
        array $values,
        ?string $filterExpression = null,
        string $hydrate = DynamoManager::HYDRATE_OBJECT
    ): mixed {
        $params = ['Limit' => 1];
        $result = $this->query($keyCondition, $values, $filterExpression, null, null, $params, $hydrate);
        return $result['items'][0] ?? null;
    }

    /**
     * Conta o número de itens que correspondem aos critérios
     */
    public function count(
        string $keyCondition,
        array $values,
        ?string $filterExpression = null
    ): int {
        $result = $this->query($keyCondition, $values, $filterExpression, null, null, ['Select' => 'COUNT']);
        return $result['Count'] ?? 0;
    }

    /**
     * Verifica se existe um item com a chave especificada
     */
    public function exists(string $pk, ?string $sk = null): bool
    {
        $item = $this->get($pk, $sk);
        return $item !== null;
    }

    /**
     * Retorna o query builder
     */
    public function getQueryBuilder(): DynamoQueryBuilder
    {
        return new DynamoQueryBuilder($this->manager, $this->itemClass);
    }

    /**
     * Retorna o manager
     */
    public function getManager(): DynamoManager
    {
        return $this->manager;
    }

    /**
     * Retorna o nome da tabela
     */
    public function getTableName(): string
    {
        return $this->table;
    }

    /**
     * Retorna a classe do item
     */
    public function getItemClass(): string
    {
        return $this->itemClass;
    }
}
