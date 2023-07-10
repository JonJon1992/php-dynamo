<?php

namespace Jonjon\PhpDynamo;

use Aws\DynamoDb\WriteRequestBatch;
use Aws\Result;
use InvalidArgumentException;
use Jonjon\PhpDynamo\Item\DynamoItem;
use Jonjon\PhpDynamo\Util\Dynamo;
use Jonjon\PhpDynamo\Util\DynamoTransaction;
use ReflectionClass;
use ReflectionException;

class DynamoManager
{
    protected $dynamo;

    protected $namespace;

    protected $transactions;

    const HYDRATE_OBJECT = 'object';
    const HYDRATE_ARRAY = 'array';
    const HYDRATE_STDCLASS = 'stdclass';


    private function __construct(
        string $key, string $secret,
        string $region,
        string $endpoint = null,
        string $namespace = null,
    )
    {
        $this->dynamo = new Dynamo($key, $secret, $region, $endpoint);
        $this->namespace = $namespace;
        $this->transactions = new DynamoTransaction($this->dynamo);
    }

    public static function create(
        string $key,
        string $secret,
        string $region,
        string $endpoint = null,
        string $namespace = null
    ): DynamoManager
    {
        return new DynamoManager($key, $secret, $region, $endpoint, $namespace);
    }

    public function save(DynamoItem $item): Result
    {
        return $this->dynamo->put($item::getTableName(), $item->serialize());
    }

    public function delete(DynamoItem $item): Result
    {
        return $this->dynamo->delete($item::getTableName(), $item->getKey());
    }

    public function getDynamo(): Dynamo
    {
        return $this->dynamo;
    }

    public function convert($item, string $hydrate = self::HYDRATE_OBJECT, string $class = null)
    {
        if (empty($item)) {
            return null;
        }
        if ($hydrate === self::HYDRATE_OBJECT) {
            $class = $this->getItemClassByItem($item, $class);
            return $this->itemToDynamoItem($item, $class);
        }
        if ($hydrate === self::HYDRATE_ARRAY) {
            return json_decode(json_encode($item, JSON_UNESCAPED_SLASHES), true);
        }
        if ($hydrate === self::HYDRATE_STDCLASS) {
            return (object)$item;
        }
        return $item;
    }

    public function convertItens(array $itens, string $hydrate = self::HYDRATE_OBJECT, string $class = null)
    {
        return array_map(function ($item) use ($hydrate, $class) {
            return $this->convert($item, $hydrate, $item);
        }, $itens);
    }

    public function query(
        string $table,
        string $keyConditionExpression,
        array  $values,
        string $filterExpression = null,
        string $attributes = null,
        array  $names = null,
        array  $params = null
    )
    {
        return $this->dynamo->query(
            $table,
            $keyConditionExpression,
            $values,
            $filterExpression,
            $attributes,
            $names,
            $params
        );
    }

    public function insertItems(array $itens, array $config = []): ?WriteRequestBatch
    {
        if (count($itens) === 0) {
            return null;
        }
        $table = $itens[0]::getTableName();
        return $this->dynamo->insertItems($table, array_map(function (DynamoItem $item) use ($table) {
            if ($item::getTableName() != $table) {
                throw new InvalidArgumentException("Itens of tables diferents.");
            }
            return $item->serialize();
        }, $itens), $config);
    }

    public function deleteItems(array $itens, array $config = []): ?WriteRequestBatch
    {
        if (count($itens) === 0) {
            return null;
        }
        $table = $itens[0]::getTableName();
        return $this->dynamo->deleteItems($table, array_map(function (DynamoItem $item) use ($table) {
            if ($item::getTableName() != $table) {
                throw new InvalidArgumentException("Itens of tables diferents.");
            }
            return $item->getKey();
        }, $itens), $config);
    }

    public function getTransaction(): DynamoTransaction
    {
        return $this->transactions;
    }

    /**
     * @param $item
     * @param string $class
     * @return string
     * @throws ReflectionException
     */
    public function itemToDynamoItem($item, string $class): string
    {
        $reflect = new ReflectionClass($class);
        return $reflect->newInstance($item);
    }

    private function getItemClassByItem($item, ?string $class): ?string
    {
        $item = (object)$item;

        if (!empty($item->ItemType) && !empty($this->itemsNamespace)) {
            $class = "$this->itemsNamespace\\$item->ItemType";

        } elseif (empty($class)) {
            throw  new InvalidArgumentException("Item type not found");
        }
        return $class;
    }
}