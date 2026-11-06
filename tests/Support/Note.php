<?php

declare(strict_types=1);

namespace Tests\Support;

use Jonjon\PhpDynamo\Item\DynamoItem;
use Jonjon\PhpDynamo\Item\DynamoProperty;

/**
 * Item usado nos testes de integração; a tabela é definida em tempo de execução.
 */
final class Note extends DynamoItem
{
    public static string $table = 'notes';

    #[DynamoProperty(['name' => 'owner', 'type' => 'string', 'required' => true])]
    private string $owner;

    #[DynamoProperty(['name' => 'slug', 'type' => 'string', 'required' => true])]
    private string $slug;

    #[DynamoProperty(['name' => 'text', 'type' => 'string'])]
    private ?string $text = null;

    public static function make(string $owner, string $slug, ?string $text = null): self
    {
        return new self(['owner' => $owner, 'slug' => $slug, 'text' => $text]);
    }

    public function pk(): string
    {
        return "OWNER#{$this->owner}";
    }

    public function sk(): string
    {
        return "NOTE#{$this->slug}";
    }

    public static function getTableName(): string
    {
        return self::$table;
    }

    public function getText(): ?string
    {
        return $this->text;
    }
}
