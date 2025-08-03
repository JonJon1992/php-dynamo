<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Item;

use Attribute;
use InvalidArgumentException;

#[Attribute(Attribute::TARGET_PROPERTY)]
class DynamoProperty
{
    public readonly string $name;
    public readonly ?string $type;
    public readonly ?string $onHydrate;
    public readonly ?string $onSerialize;
    public readonly bool $required;

    /**
     * Construtor da classe
     */
    public function __construct(array $values)
    {
        if (!isset($values['name'])) {
            throw new InvalidArgumentException("O atributo 'name' é obrigatório.");
        }

        $this->name = $values['name'];
        $this->type = $values['type'] ?? null;
        $this->onHydrate = $values['onHydrate'] ?? null;
        $this->onSerialize = $values['onSerialize'] ?? null;
        $this->required = isset($values['required']) && $values['required'];
    }

    /**
     * Retorna o nome do atributo
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Retorna o tipo do atributo
     */
    public function getType(): ?string
    {
        return $this->type;
    }

    /**
     * Retorna o método de hidratação
     */
    public function getOnHydrate(): ?string
    {
        return $this->onHydrate;
    }

    /**
     * Retorna o método de serialização
     */
    public function getOnSerialize(): ?string
    {
        return $this->onSerialize;
    }

    /**
     * Verifica se o atributo é obrigatório
     */
    public function isRequired(): bool
    {
        return $this->required;
    }

    /**
     * Cria uma instância do DynamoProperty com nome
     */
    public static function create(string $name, array $options = []): self
    {
        return new self(array_merge(['name' => $name], $options));
    }

    /**
     * Cria um atributo obrigatório
     */
    public static function required(string $name, ?string $type = null): self
    {
        return new self([
            'name' => $name,
            'type' => $type,
            'required' => true
        ]);
    }

    /**
     * Cria um atributo opcional
     */
    public static function optional(string $name, ?string $type = null): self
    {
        return new self([
            'name' => $name,
            'type' => $type,
            'required' => false
        ]);
    }

    /**
     * Cria um atributo com método de hidratação
     */
    public static function withHydrate(string $name, string $method, ?string $type = null): self
    {
        return new self([
            'name' => $name,
            'type' => $type,
            'onHydrate' => $method
        ]);
    }

    /**
     * Cria um atributo com método de serialização
     */
    public static function withSerialize(string $name, string $method, ?string $type = null): self
    {
        return new self([
            'name' => $name,
            'type' => $type,
            'onSerialize' => $method
        ]);
    }
}