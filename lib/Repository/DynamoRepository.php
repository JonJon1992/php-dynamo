<?php

namespace Jonjon\PhpDynamo\Repository;

use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\DynamoQueryBuilder;

abstract class DynamoRepository
{
    private $manager;

    private $table;

    private $itemClass;

    public function __construct(string $itemClass, DynamoManager $manager)
    {
        $this->manager = $manager;
        $this->itemClass = $itemClass;
        $this->table = $itemClass::getTableName();
    }

    public function get(
        string $pk,
        string $sk = null,
        string $hydrate = DynamoManager::HYDRATE_OBJECT
    )
    {
        $key = ['PK' => $pk];
        if ($sk) {
            $key['SK'] = $sk;
        }
        $item = $this->manager->getDynamo()->get($this->table, $key);
        return $this->manager->convert($item, $hydrate, $this->itemClass);
    }

    public function query(
        string $keyCondition,
        array  $values,
        string $filterExpression = null,
        string $attributes = null,
        array  $names = null,
        array  $params = null,
        string $hydrate = DynamoManager::HYDRATE_OBJECT
    ): array
    {
        $result = $this->manager->getDynamo()->query(
            $this->table,
            $keyCondition,
            $values,
            $filterExpression,
            $attributes,
            $names,
            $params
        );
        $result['items'] = $this->manager->convertItens($result['items'], $hydrate, $this->itemClass);
        return $result;
    }

    public function scan(
        string $filterExpression = null,
        array  $values = null,
        array  $params = null
    ): array
    {
        return $this->manager->getDynamo()->scan($this->table, $filterExpression, $values, $params);
    }

    public function getQueryBuilder(): DynamoQueryBuilder
    {
        return new DynamoQueryBuilder($this->manager, $this->itemClass);
    }
}