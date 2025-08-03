<?php

declare(strict_types=1);

namespace Examples;

use Jonjon\PhpDynamo\Item\DynamoItem;
use Jonjon\PhpDynamo\Item\DynamoProperty;

class User extends DynamoItem
{
    #[DynamoProperty(['name' => 'email', 'type' => 'string', 'required' => true])]
    private string $email;

    #[DynamoProperty(['name' => 'name', 'type' => 'string', 'required' => true])]
    private string $name;

    #[DynamoProperty(['name' => 'age', 'type' => 'integer', 'required' => false])]
    private ?int $age = null;

    #[DynamoProperty(['name' => 'active', 'type' => 'boolean', 'required' => false])]
    private bool $active = true;

    #[DynamoProperty(['name' => 'created_at', 'type' => 'string', 'required' => false])]
    private string $createdAt;

    #[DynamoProperty(['name' => 'updated_at', 'type' => 'string', 'required' => false])]
    private string $updatedAt;

    public function __construct($data = null)
    {
        parent::__construct($data);
        
        if ($data === null) {
            $this->createdAt = date('Y-m-d H:i:s');
            $this->updatedAt = date('Y-m-d H:i:s');
        }
    }

    public function pk(): string
    {
        return "USER#{$this->email}";
    }

    public function sk(): ?string
    {
        return "PROFILE#{$this->email}";
    }

    public static function getTableName(): string
    {
        return 'users';
    }

    // Getters
    public function getEmail(): string
    {
        return $this->email;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getAge(): ?int
    {
        return $this->age;
    }

    public function isActive(): bool
    {
        return $this->active;
    }

    public function getCreatedAt(): string
    {
        return $this->createdAt;
    }

    public function getUpdatedAt(): string
    {
        return $this->updatedAt;
    }

    // Setters
    public function setEmail(string $email): self
    {
        $this->email = $email;
        return $this;
    }

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function setAge(?int $age): self
    {
        $this->age = $age;
        return $this;
    }

    public function setActive(bool $active): self
    {
        $this->active = $active;
        return $this;
    }

    public function setCreatedAt(string $createdAt): self
    {
        $this->createdAt = $createdAt;
        return $this;
    }

    public function setUpdatedAt(string $updatedAt): self
    {
        $this->updatedAt = $updatedAt;
        return $this;
    }

    // Método para atualizar o timestamp de modificação
    public function touch(): self
    {
        $this->updatedAt = date('Y-m-d H:i:s');
        return $this;
    }

    // Método para verificar se o usuário é maior de idade
    public function isAdult(): bool
    {
        return $this->age !== null && $this->age >= 18;
    }

    // Método para obter informações básicas do usuário
    public function getBasicInfo(): array
    {
        return [
            'email' => $this->email,
            'name' => $this->name,
            'active' => $this->active
        ];
    }
} 