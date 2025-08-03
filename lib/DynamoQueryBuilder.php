<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo;

use ReflectionClass;

class DynamoQueryBuilder
{
    private string $table;
    private string $class;
    private DynamoManager $manager;
    private ?string $keyConditionExpression = null;
    private array $values = [];
    private ?string $filterExpression = null;
    private string $attributes = '';
    private array $names = [];
    private array $params = [];
    private ?array $lastKey = null;
    private array $items = [];

    public function __construct(DynamoManager $manager, string $class)
    {
        $this->table = $class::getTableName();
        $this->manager = $manager;
        $this->class = $class;
    }

    /**
     * Define a condição de chave
     */
    public function setCondition(string $keyCondition): static
    {
        $this->keyConditionExpression = $keyCondition;
        return $this;
    }

    /**
     * Adiciona um valor para a query
     */
    public function addValue(string $key, mixed $value): static
    {
        $this->values[$key] = $value;
        return $this;
    }

    /**
     * Define o filtro da query
     */
    public function setFilter(string $filter): static
    {
        $this->filterExpression = $filter;
        return $this;
    }

    /**
     * Define os atributos a serem retornados
     */
    public function setAttributes(string $attributes): static
    {
        $this->attributes = $attributes;
        return $this;
    }

    /**
     * Adiciona parâmetros adicionais
     */
    public function addParams(string $key, mixed $value): static
    {
        $this->params[$key] = $value;
        return $this;
    }

    /**
     * Adiciona nomes de atributos
     */
    public function addAttributeName(string $key, string $value): static
    {
        $this->names[$key] = $value;
        return $this;
    }

    /**
     * Retorna o índice configurado
     */
    public function getIndex(): ?string
    {
        return $this->params['IndexName'] ?? null;
    }

    /**
     * Define o índice a ser usado
     */
    public function setIndex(string $index): static
    {
        $this->params['IndexName'] = $index;
        return $this;
    }

    /**
     * Define a chave de início para paginação
     */
    public function setStartKey(array $startKey): static
    {
        $this->params['ExclusiveStartKey'] = $startKey;
        return $this;
    }

    /**
     * Retorna o limite configurado
     */
    public function getLimit(): ?int
    {
        return $this->params['Limit'] ?? null;
    }

    /**
     * Define o limite de resultados
     */
    public function setLimit(int $limit): static
    {
        $this->params['Limit'] = $limit;
        return $this;
    }

    /**
     * Define ordenação decrescente
     */
    public function setOrderByDesc(): static
    {
        $this->params['ScanIndexForward'] = false;
        return $this;
    }

    /**
     * Define ordenação crescente
     */
    public function setOrderByAsc(): static
    {
        $this->params['ScanIndexForward'] = true;
        return $this;
    }

    /**
     * Retorna a última chave para paginação
     */
    public function getLastKey(): ?array
    {
        return $this->lastKey;
    }

    /**
     * Retorna os itens da query
     */
    public function getItems(): array
    {
        return $this->items;
    }

    /**
     * Verifica se é a última página
     */
    public function isLastPage(): bool
    {
        return $this->getLastKey() === null;
    }

    /**
     * Executa a query e retorna os resultados
     */
    public function getResult(string $hydrate = DynamoManager::HYDRATE_OBJECT, bool $filterItem = false): array
    {
        if ($filterItem) {
            $this->addItemTypeFilter();
        }

        $result = $this->manager->query(
            $this->table,
            $this->keyConditionExpression,
            $this->values,
            $this->filterExpression,
            $this->attributes,
            $this->names,
            $this->params,
            $this->class
        );

        $this->lastKey = $result['lastKey'];
        $this->items = $result['items'];

        return $result;
    }

    /**
     * Executa a query e retorna apenas os itens
     */
    public function getItemsResult(string $hydrate = DynamoManager::HYDRATE_OBJECT, bool $filterItem = false): array
    {
        $result = $this->getResult($hydrate, $filterItem);
        return $result['items'];
    }

    /**
     * Executa a query e retorna o primeiro item
     */
    public function getFirstResult(string $hydrate = DynamoManager::HYDRATE_OBJECT, bool $filterItem = false)
    {
        $this->setLimit(1);
        $result = $this->getResult($hydrate, $filterItem);
        return $result['items'][0] ?? null;
    }

    /**
     * Adiciona filtro por tipo de item
     */
    private function addItemTypeFilter(): void
    {
        $reflect = new ReflectionClass($this->class);
        $itemType = $reflect->getShortName();
        
        $filterClass = ['ItemType = :filterItemClass'];
        
        if ($this->filterExpression) {
            $filterClass[] = "({$this->filterExpression})";
        }
        
        $this->filterExpression = implode(' AND ', $filterClass);
        $this->addValue(':filterItemClass', $itemType);
    }

    /**
     * Reseta o builder para uma nova query
     */
    public function reset(): static
    {
        $this->keyConditionExpression = null;
        $this->values = [];
        $this->filterExpression = null;
        $this->attributes = '';
        $this->names = [];
        $this->params = [];
        $this->lastKey = null;
        $this->items = [];
        
        return $this;
    }
}