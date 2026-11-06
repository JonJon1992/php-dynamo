<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Util;

use Aws\Result;
use Jonjon\PhpDynamo\Item\DynamoItem;
use OverflowException;

class DynamoTransaction
{
    /**
     * Limite do TransactWriteItems no DynamoDB
     */
    public const MAX_OPERATIONS = 100;

    protected Dynamo $dynamo;
    /** @var list<array<string, array<string, mixed>>> */
    protected array $operations = [];

    public function __construct(Dynamo $dynamo)
    {
        $this->dynamo = $dynamo;
    }

    /**
     * Adiciona uma operação PUT
     */
    public function put(string $table, array|object|string $doc): static
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
     *
     * @param array<string, string>|null $names
     */
    public function delete(
        string $table,
        array $key,
        ?string $condition = null,
        ?array $values = null,
        ?array $names = null
    ): static {
        $this->operations[] = [
            'Delete' => $this->dynamo->toPrepareDelete($table, $key, $condition, $values, $names)
        ];
        return $this;
    }

    /**
     * Adiciona uma operação UPDATE
     *
     * @param array<string, string>|null $names
     */
    public function update(
        string $table,
        array $key,
        string $updateExpression,
        array $values,
        ?string $condition = null,
        ?array $names = null
    ): static {
        $this->operations[] = [
            'Update' => $this->dynamo->toPrepareUpdate($table, $updateExpression, $values, $key, $condition, $names)
        ];
        return $this;
    }

    /**
     * Executa a transação
     */
    public function flush(): Result
    {
        if (count($this->operations) > self::MAX_OPERATIONS) {
            throw new OverflowException(
                sprintf('Limite de %d operações por transação excedido.', self::MAX_OPERATIONS)
            );
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
