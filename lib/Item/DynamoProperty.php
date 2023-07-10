<?php

namespace Jonjon\PhpDynamo\Item;

use Doctrine\Common\Annotations\AnnotationReader;
use ReflectionProperty;

class DynamoProperty
{

    /**
     * @var string
     */
    public $name;
    /**
     * @var string
     */
    public $type;
    /**
     * @var string
     */

    public $onHydrate;

    public $onSerialize;

    /**
     * @var bool
     */
    public $required;

    /**
     * @var array
     */
    private static $propAnnotation = [];

    public function __construct(array $values)
    {
        if (!isset($values['name'])) {
            throw new InvalidArgumentException("The attribute 'name' is required.");
        }

        $this->name = $values['name'];
        $this->type = $values['type'] ?? null;
        $this->onHydrate = $values['onHydrate'] ?? null;
        $this->onSerialize = $values['onSerialize'] ?? null;
        $this->required = isset($values['required']) && $values['required'];
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string|null
     */
    public function getType(): ?string
    {
        return $this->type;
    }

    /**
     * @return string|null
     */
    public function getOnHydrate(): ?string
    {
        return $this->onHydrate;
    }


    public function getOnSerialize(): ?string
    {
        return $this->onSerialize;
    }

    /**
     * @return bool
     */
    public function isRequired(): bool
    {
        return $this->required;
    }


    public static function getAnnotation(string $class, ReflectionProperty $property, AnnotationReader $annotation)
    {
        $name = $property->getName();
        if (isset(self::$propAnnotation[$class][$name])) {
            return self::$propAnnotation[$class][$name];
        }
        self::$propAnnotation[$class][$name] = $annotation->getPropertyAnnotation($property, DynamoProperty::class);
        return self::$propAnnotation[$class][$name];
    }

}