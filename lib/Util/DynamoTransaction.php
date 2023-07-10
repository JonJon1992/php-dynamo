<?php

namespace Jonjon\PhpDynamo\Util;

use Aws\Result;
use Jonjon\PhpDynamo\Item\DynamoItem;
use OverflowException;
use ReflectionException;

class DynamoTransaction
{
    protected $dynamo;

    protected $operations;

    public function __construct(Dynamo $dynamo)
    {
        $this->dynamo = $dynamo;
    }

    public function put(string $table, $doc): static
    {
        $this->operations[] = [
            'Put' => $this->dynamo->toPrepare($table, $doc)
        ];

        return $this;
    }

    /**
     * @throws ReflectionException
     */
    public function save(DynamoItem $item): static
    {
        return $this->put($item::getTableName(), $item->serialize());
    }

    public function deleteItem(DynamoItem $item): static
    {
        return $this->delete($item::getTableName(), $item->getKey());
    }


    public function delete(string $table, array $key, $condition = null, array $values = null): static
    {
        $this->operations[] = [
            'Delete' => $this->dynamo->toPrepareDelete($table, $key, $condition, $values)
        ];
        return $this;
    }

    public function update(string $table, array $key, string $updateExpression, array $values, string $condition = null): static
    {
        $this->operations[] = [
            'Update' => $this->dynamo->toPrepareUpdate($table, $updateExpression, $values, $key, $condition)
        ];
        return $this;
    }

    public function flush(): Result
    {
        if (count($this->operations) > 25) {
            throw  new OverflowException('Limit of 25 operations per transations exceeded.');
        }
        $result = $this->dynamo->getClient()->transactWriteItems([
            'TransactItems' => $this->operations
        ]);
        $this->operations = [];

        return $result;
    }
}