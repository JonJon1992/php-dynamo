<?php

namespace Jonjon\PhpDynamo;

use ReflectionClass;

class DynamoQueryBuilder
{
    private $table;
    private $class;

    private $manager;

    private $keyConditionExpression = null;

    private $values = [];

    private $filterExpression = null;
    private $attributes = '';

    private $names = [];

    private $params = [];

    private $lastKey = null;
    private $items = [];

    public function __construct(DynamoManager $manager, string $class)
    {
        $this->table = $class::getTableName();
        $this->manager = $manager;
        $this->class = $class;
    }

    /**
     * @param $keyCondition
     * @return $this
     */
    public function setCondition($keyCondition): static
    {
        $this->keyConditionExpression = $keyCondition;
        return $this;
    }

    /**
     * @param $key
     * @param $value
     * @return $this
     */
    public function addValue($key, $value): static
    {
        $this->values[$key] = $value;
        return $this;
    }

    /**
     * @param $filter
     * @return $this
     */
    public function setFilter($filter): static
    {
        $this->filterExpression = $filter;
        return $this;
    }

    /**
     * @param $attributes
     * @return $this
     */
    public function setAttributes($attributes): static
    {
        $this->attributes = $attributes;
        return $this;
    }

    /**
     * @param $key
     * @param $value
     * @return $this
     */
    public function addParams($key, $value): static
    {
        $this->params[$key] = $value;
        return $this;
    }

    /**
     * @param $key
     * @param $value
     * @return $this
     */
    public function addAttributeName($key, $value): static
    {
        $this->params[$key] = $value;
        return $this;
    }

    /**
     * @return mixed|null
     */
    public function getIndex(): mixed
    {
        return $this->params['IndexName'] ?? null;
    }

    /**
     * @param $index
     * @return $this
     */
    public function setIndex($index): static
    {
        $this->params['IndexName'] = $index;
        return $this;
    }

    /**
     * @param array $startKey
     * @return $this
     */
    public function setStartKey(array $startKey): static
    {
        $this->params['ExclusiveStartKey'] = $startKey;
        return $this;
    }

    /**
     * @return mixed|null
     */
    public function getLimit(): mixed
    {
        return $this->params['Limit'] ?? null;
    }

    /**
     * @param int $limit
     * @return $this
     */
    public function setLimit(int $limit): static
    {
        $this->params['Limit'] = $limit;
        return $this;
    }

    /**
     * @return $this
     */
    public function setOrderByDes(): static
    {
        $this->params['ScanIndexForward'] = false;
        return $this;
    }

    /**
     * @return null
     */
    public function getLastKey()
    {
        return $this->lastKey;
    }

    /**
     * @return array
     */
    public function getItems(): array
    {
        return $this->items;
    }

    /**
     * @return bool
     */
    public function isLastPage(): bool
    {
        return is_null($this->getLastKey());
    }

    /**
     * @param string $hydrate
     * @param bool $filterItem
     * @return array
     */
    public function getResult(string $hydrate = DynamoManager::HYDRATE_OBJECT, bool $filterItem = false): array
    {
        if ($filterItem) {
            $filterClass = ['ItemType= :filterItemClass'];
            if ($this->filterExpression) {
                $filterClass[] = "({$this->filterExpression})";

            }
            $this->filterExpression = implode(' AND ', $filterClass);
            $reflect = new ReflectionClass($this->class);
            $this->addValue(':filterItemClass', $reflect->getShortName());
        }
        $result = $this->manager->query(
            $this->table,
            $this->keyConditionExpression,
            $this->values,
            $this->filterExpression,
            $this->attributes,
            $this->names,
            $this->params
        );
        $result['items'] = $this->manager->convertItens($result['items'], $hydrate, $this->class);

        $this->lastKey = $result['lastKey'];
        $this->items = $result['items'];

        return $result;
    }

}