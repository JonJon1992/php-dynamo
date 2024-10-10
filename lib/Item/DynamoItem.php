<?php

namespace Jonjon\PhpDynamo\Item;

use Doctrine\Common\Annotations\AnnotationReader;
use InvalidArgumentException;
use ReflectionClass;
use ReflectionException;
use ReflectionMethod;
use stdClass;

abstract class DynamoItem
{
    /**
     * @param $data
     */
    public function __construct($data = null)
    {
    }

    abstract function pk(): string;

    abstract function sk(): ?string;

    abstract static function getTableName(): string;

    public function itemType(): string
    {
        $reflect = new ReflectionClass(get_class($this));
        return $reflect->getShortName();
    }

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
     * @throws ReflectionException
     */
    private function setDate($data): static
    {
        if (is_array($data)) {
            $data = (object)$data;
        } elseif (is_string($data)) {
            $data = json_decode($data);
        }
        if (is_object($data)) {
            $this->hydrate($data);
        }
        return $this;
    }

    /**
     * @throws ReflectionException
     */
    private function hydrate(stdClass $data): void
    {
        $class = get_class($this);
        $annotation = new AnnotationReader();
        $rClass = new ReflectionClass($class);
        $properties = $rClass->getProperties();

        array_map(/**
         * @throws ReflectionException
         */ function ($rcProp) use ($annotation, $data, $class) {

            $doc = DynamoProperty::getAnnotation($class, $rcProp, $annotation);
            if (!$doc) {
                return;
            }
            $name = $doc->getName();
            $required = $doc->isRequired();

            $propExist = property_exists($data, $name);

            if (!$propExist && !$required) {
                return;
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
                $rcProp->setValue($this);
                $rcProp->setAccessible($isAccessible);
            }
        },
            $properties);
    }

    /**
     * @throws ReflectionException
     */
    public function serialize(): stdClass
    {
        $data = new stdClass();

        $this->runIndex($data);

        $data->ItemType = $this->itemType();

        $data->PK = $this->pk();

        if (!empty($this->sk())) {
            $data->SK = $this->sk();
        }
        $class = get_class($this);

        $annotation = new AnnotationReader();
        $rClass = new ReflectionClass($class);
        $properties = $rClass->getProperties();

        array_map(/**
         * @throws ReflectionException
         */ function ($rcProp) use ($annotation, $data, $class) {

            $doc = DynamoProperty::getAnnotation($class, $rcProp, $annotation);
            if (!$doc) {
                return;
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
        }, $properties);

        return $data;
    }

    private function check(bool $required, $value, string $name): void
    {
        $isBlank = function ($value) {
            return empty($value) && !is_numeric($value) && !is_bool($value);
        };
        if ($isBlank($value) && $required) {
            $value = !is_scalar($value) ? json_encode($value) : $value;
            throw  new InvalidArgumentException("The attribute '$name' is required, '$value' received");
        }
    }

    private function cast(?string $type, $value, string $name)
    {
        if ($type && !is_null($value) && !settype($value, $type)) {
            throw new InvalidArgumentException("The type info at property '$name' is invalid,'$type' received");
        }
        return $value;
    }

    private function runIndex(stdClass $data): void
    {
        $methods = get_class_methods(get_class($this));

        foreach ($methods as $method) {
            $method = ucfirst($method);
            $prefix = strtoupper(substr($method, 0, 3));

            if (($prefix == 'GSI' || $prefix == 'LSI') && $this->$method()) {
                $index = $this->$method();

                $pk = $method . 'Pk';
                $sk = $method . 'Sk';

                $data->$pk = $index['PK'];
                $data->$sk = $index['SK'];
            }
        }
    }
}