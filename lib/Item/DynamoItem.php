<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Item;

use InvalidArgumentException;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;
use stdClass;

/**
 * @phpstan-consistent-constructor
 */
abstract class DynamoItem
{
    private static array $indexMethodsCache = [];
    private static array $propertyMetadataCache = [];
    private static array $shortNameCache = [];

    /**
     * Construtor da classe
     */
    public function __construct(mixed $data = null)
    {
        if ($data !== null) {
            $this->setData($data);
        }
    }

    /**
     * Define a chave primária (PK)
     */
    abstract public function pk(): string;

    /**
     * Define a chave de ordenação (SK) - opcional
     */
    abstract public function sk(): ?string;

    /**
     * Define o nome da tabela
     */
    abstract public static function getTableName(): string;

    /**
     * Retorna o tipo do item baseado no nome da classe
     */
    public function itemType(): string
    {
        $class = get_class($this);

        return self::$shortNameCache[$class] ??= (new ReflectionClass($class))->getShortName();
    }

    /**
     * Retorna a chave do item (PK e SK)
     */
    public function getKey(): array
    {
        $key = [
            'PK' => $this->pk(),
        ];

        $sk = $this->sk();
        if (!empty($sk)) {
            $key['SK'] = $sk;
        }

        return $key;
    }

    /**
     * Define os dados do item
     */
    private function setData(mixed $data): static
    {
        if (is_array($data)) {
            $data = (object)$data;
        } elseif (is_string($data)) {
            $data = json_decode($data);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new InvalidArgumentException("JSON inválido: " . json_last_error_msg());
            }
        }

        if (is_object($data)) {
            $this->hydrate($data);
        }

        return $this;
    }

    /**
     * Hidrata o objeto com os dados
     */
    private function hydrate(object $data): void
    {
        $class = get_class($this);

        foreach ($this->getPropertyMetadata($class) as $metadata) {
            /** @var DynamoProperty $doc */
            $doc = $metadata['doc'];

            $name = $doc->getName();
            $required = $doc->isRequired();

            $propExist = property_exists($data, $name);

            if (!$propExist && !$required) {
                continue;
            }

            $value = $propExist ? $data->{$name} : null;

            $this->check($required, $value, $name);
            $value = $this->cast($doc->getType(), $value, $name);

            if ($metadata['hydrateMethod'] instanceof ReflectionMethod) {
                $metadata['hydrateMethod']->invoke($this, $value);
            } else {
                $metadata['property']->setValue($this, $value);
            }
        }
    }

    /**
     * Serializa o item para o formato do DynamoDB
     */
    public function serialize(): stdClass
    {
        $data = new stdClass();

        $this->runIndex($data);

        $data->ItemType = $this->itemType();

        // Forçar PK e SK como string
        $data->PK = (string) $this->pk();

        $sk = $this->sk();
        if (!empty($sk)) {
            $data->SK = (string) $sk;
        }

        $class = get_class($this);

        foreach ($this->getPropertyMetadata($class) as $metadata) {
            /** @var DynamoProperty $doc */
            $doc = $metadata['doc'];

            if ($metadata['serializeMethod'] instanceof ReflectionMethod) {
                $value = $metadata['serializeMethod']->invoke($this);
            } else {
                /** @var ReflectionProperty $property */
                $property = $metadata['property'];
                $value = $property->isInitialized($this) ? $property->getValue($this) : null;
            }

            $name = $doc->getName();
            $this->check($doc->isRequired(), $value, $name);
            $data->{$name} = $this->cast($doc->getType(), $value, $name);
        }

        return $data;
    }

    /**
     * Verifica se um valor obrigatório está presente
     */
    private function check(bool $required, mixed $value, string $name): void
    {
        $isBlank = function ($value) {
            return empty($value) && !is_numeric($value) && !is_bool($value);
        };

        if ($isBlank($value) && $required) {
            $value = !is_scalar($value) ? json_encode($value) : $value;
            throw new InvalidArgumentException("O atributo '$name' é obrigatório, recebido: '$value'");
        }
    }

    /**
     * Faz o cast do valor para o tipo especificado
     */
    private function cast(?string $type, mixed $value, string $name): mixed
    {
        if ($type && !is_null($value) && !settype($value, $type)) {
            throw new InvalidArgumentException("O tipo do atributo '$name' é inválido, recebido: '$type'");
        }
        return $value;
    }

    /**
     * Processa índices secundários (GSI/LSI)
     */
    private function runIndex(stdClass $data): void
    {
        $class = get_class($this);
        $methods = self::$indexMethodsCache[$class] ??= $this->getIndexMethods($class);

        foreach ($methods as $method) {
            $index = $this->$method();

            if (is_array($index) && isset($index['PK'])) {
                $pk = $method . 'Pk';
                $sk = $method . 'Sk';

                $data->$pk = $index['PK'];
                $data->$sk = $index['SK'];
            }
        }
    }

    /**
     * Obtém a anotação da propriedade usando PHP 8 attributes
     */
    private function getPropertyAnnotation(string $class, ReflectionProperty $property): ?DynamoProperty
    {
        $attributes = $property->getAttributes(DynamoProperty::class);
        if (!empty($attributes)) {
            return $attributes[0]->newInstance();
        }

        return null;
    }

    private function getIndexMethods(string $class): array
    {
        $indexMethods = [];

        foreach (get_class_methods($class) as $method) {
            $method = ucfirst($method);
            $prefix = strtoupper(substr($method, 0, 3));

            if (($prefix === 'GSI' || $prefix === 'LSI') && method_exists($this, $method)) {
                $indexMethods[] = $method;
            }
        }

        return $indexMethods;
    }

    private function getPropertyMetadata(string $class): array
    {
        if (isset(self::$propertyMetadataCache[$class])) {
            return self::$propertyMetadataCache[$class];
        }

        $metadata = [];
        $rClass = new ReflectionClass($this);

        foreach ($rClass->getProperties() as $property) {
            $doc = $this->getPropertyAnnotation($class, $property);
            if (!$doc) {
                continue;
            }

            $hydrateMethod = $doc->getOnHydrate() ? new ReflectionMethod($class, $doc->getOnHydrate()) : null;
            $serializeMethod = $doc->getOnSerialize() ? new ReflectionMethod($class, $doc->getOnSerialize()) : null;

            $metadata[] = [
                'property' => $property,
                'doc' => $doc,
                'hydrateMethod' => $hydrateMethod,
                'serializeMethod' => $serializeMethod,
            ];
        }

        return self::$propertyMetadataCache[$class] = $metadata;
    }

    private function normalizeForArray(mixed $value): mixed
    {
        if ($value instanceof stdClass) {
            return $this->normalizeForArray(get_object_vars($value));
        }

        if (is_array($value)) {
            return array_map(fn($item) => $this->normalizeForArray($item), $value);
        }

        return $value;
    }

    /**
     * Converte o item para array
     */
    public function toArray(): array
    {
        return $this->normalizeForArray($this->serialize());
    }

    /**
     * Converte o item para JSON
     */
    public function toJson(): string
    {
        return json_encode($this->serialize(), JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR);
    }

    /**
     * Cria uma instância a partir de um array
     */
    public static function fromArray(array $data): static
    {
        return new static($data);
    }

    /**
     * Cria uma instância a partir de JSON
     */
    public static function fromJson(string $json): static
    {
        return new static($json);
    }
}
