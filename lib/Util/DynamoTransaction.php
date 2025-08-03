<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Util;

use Aws\Result;
use Jonjon\PhpDynamo\Item\DynamoItem;
use OverflowException;
use ReflectionException;

class DynamoTransaction
{
    protected Dynamo $dynamo;
    protected array $operations = [];

    public function __construct(Dynamo $dynamo)
    {
        $this->dynamo = $dynamo;
    }

    /**
     * Adiciona uma operação PUT
     */
    public function put(string $table, $doc): static
    {
        $this->operations[] = [
            'Put' => $this->dynamo->toPrepare($table, $doc)
        ];

        return $this;
    }

    /**
     * Adiciona uma operação SAVE
     */
    public function save(DynamoItem $item): static
    {
        return $this->put($item::getTableName(), $item->serialize());
    }

    /**
     * Adiciona uma operação DELETE para um item
     */
    public function deleteItem(DynamoItem $item): static
    {
        return $this->delete($item::getTableName(), $item->getKey());
    }

    /**
     * Adiciona uma operação DELETE
     */
    public function delete(string $table, array $key, ?string $condition = null, ?array $values = null): static
    {
        $this->operations[] = [
            'Delete' => $this->dynamo->toPrepareDelete($table, $key, $condition, $values)
        ];
        return $this;
    }

    /**
     * Adiciona uma operação UPDATE
     */
    public function update(string $table, array $key, string $updateExpression, array $values, ?string $condition = null): static
    {
        $this->operations[] = [
            'Update' => $this->dynamo->toPrepareUpdate($table, $updateExpression, $values, $key, $condition)
        ];
        return $this;
    }

    /**
     * Executa a transação
     */
    public function flush(): Result
    {
        if (count($this->operations) > 25) {
            throw new OverflowException('Limite de 25 operações por transação excedido.');
        }
        
        if (empty($this->operations)) {
            throw new OverflowException('Nenhuma operação para executar.');
        }
        
        $result = $this->dynamo->getClient()->transactWriteItems([
            'TransactItems' => $this->operations
        ]);
        
        $this->operations = [];

        return $result;
    }

    /**
     * Retorna o número de operações na transação
     */
    public function getOperationCount(): int
    {
        return count($this->operations);
    }

    /**
     * Limpa todas as operações da transação
     */
    public function clear(): static
    {
        $this->operations = [];
        return $this;
    }

    /**
     * Verifica se a transação está vazia
     */
    public function isEmpty(): bool
    {
        return empty($this->operations);
    }

    /**
     * Retorna as operações da transação
     */
    public function getOperations(): array
    {
        return $this->operations;
    }
}