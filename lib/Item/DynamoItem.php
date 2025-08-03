<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Item;

use InvalidArgumentException;
use ReflectionClass;
use ReflectionException;
use ReflectionMethod;
use ReflectionProperty;
use stdClass;

abstract class DynamoItem
{
    /**
     * Construtor da classe
     */
    public function __construct($data = null)
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
        $reflect = new ReflectionClass(get_class($this));
        return $reflect->getShortName();
    }

    /**
     * Retorna a chave do item (PK e SK)
     */
    public function getKey(): array
    {
        $key = [
            'PK' => $this->pk(),
        ];
        
        if (!empty($this->sk())) {
            $key['SK'] = $this->sk();
        }
        
        return $key;
    }

    /**
     * Define os dados do item
     */
    private function setData($data): static
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
    private function hydrate(stdClass $data): void
    {
        $class = get_class($this);
        $rClass = new ReflectionClass($class);
        $properties = $rClass->getProperties();

        foreach ($properties as $rcProp) {
            $doc = $this->getPropertyAnnotation($class, $rcProp);
            if (!$doc) {
                continue;
            }
            
            $name = $doc->getName();
            $required = $doc->isRequired();

            $propExist = property_exists($data, $name);

            if (!$propExist && !$required) {
                continue;
            }

            $value = $propExist ? $data->{$name} : null;

            $this->check($required, $value, $name);
            $value = $this->cast($doc->getType(), $value, $name);

            if ($doc->getOnHydrate()) {
                $rMethod = new ReflectionMethod($class, $doc->getOnHydrate());
                $rMethod->invoke($this, $value);
            } else {
                $isAccessible = $rcProp->isPublic();
                if (!$isAccessible) {
                    $rcProp->setAccessible(true);
                }
                $rcProp->setValue($this, $value);
                $rcProp->setAccessible($isAccessible);
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

        if (!empty($this->sk())) {
            $data->SK = (string) $this->sk();
        }
        
        $class = get_class($this);
        $rClass = new ReflectionClass($class);
        $properties = $rClass->getProperties();

        foreach ($properties as $rcProp) {
            $doc = $this->getPropertyAnnotation($class, $rcProp);
            if (!$doc) {
                continue;
            }

            if ($doc->getOnSerialize()) {
                $rMethod = new ReflectionMethod($class, $doc->getOnSerialize());
                $value = $rMethod->invoke($this);
            } else {
                $isAccessible = $rcProp->isPublic();
                if (!$isAccessible) {
                    $rcProp->setAccessible(true);
                }
                $value = $rcProp->getValue($this);
                $rcProp->setAccessible($isAccessible);
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
    private function check(bool $required, $value, string $name): void
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
    private function cast(?string $type, $value, string $name)
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
        $methods = get_class_methods(get_class($this));

        foreach ($methods as $method) {
            $method = ucfirst($method);
            $prefix = strtoupper(substr($method, 0, 3));

            if (($prefix === 'GSI' || $prefix === 'LSI') && method_exists($this, $method)) {
                $index = $this->$method();

                if (is_array($index) && isset($index['PK'])) {
                    $pk = $method . 'Pk';
                    $sk = $method . 'Sk';

                    $data->$pk = $index['PK'];
                    $data->$sk = $index['SK'];
                }
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
            $attribute = $attributes[0];
            $args = $attribute->getArguments();
            return new DynamoProperty($args);
        }

        return null;
    }

    /**
     * Converte o item para array
     */
    public function toArray(): array
    {
        return json_decode(json_encode($this->serialize(), JSON_UNESCAPED_SLASHES), true);
    }

    /**
     * Converte o item para JSON
     */
    public function toJson(): string
    {
        return json_encode($this->serialize(), JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
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