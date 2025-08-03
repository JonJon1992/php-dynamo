<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Util;

trait DynamoUtil
{
    /**
     * Gera valores de filtro para queries
     */
    protected function generateFilterValues(string $name, $values, string $type, $queryBuilder): void
    {
        if (is_array($values)) {
            foreach ($values as $index => $value) {
                $placeholder = ":{$name}_{$index}";
                $queryBuilder->addValue($placeholder, $value);
            }
        } else {
            $placeholder = ":{$name}";
            $queryBuilder->addValue($placeholder, $values);
        }
    }

    /**
     * Escapa caracteres especiais em expressões DynamoDB
     */
    protected function escapeExpression(string $expression): string
    {
        return str_replace(['#', ':', ' '], ['##', '::', '\\ '], $expression);
    }

    /**
     * Gera um nome de atributo único
     */
    protected function generateAttributeName(string $baseName): string
    {
        return "#" . $baseName . "_" . uniqid();
    }

    /**
     * Gera um valor de atributo único
     */
    protected function generateAttributeValue(string $baseName): string
    {
        return ":" . $baseName . "_" . uniqid();
    }

    /**
     * Converte um array para expressão de atributos
     */
    protected function arrayToAttributeExpression(array $attributes): array
    {
        $names = [];
        $expression = [];
        
        foreach ($attributes as $attribute) {
            $name = $this->generateAttributeName($attribute);
            $names[$name] = $attribute;
            $expression[] = $name;
        }
        
        return [
            'names' => $names,
            'expression' => implode(', ', $expression)
        ];
    }

    /**
     * Valida se uma chave é válida para DynamoDB
     */
    protected function validateKey(array $key): bool
    {
        if (empty($key)) {
            return false;
        }
        
        foreach ($key as $attributeName => $value) {
            if (empty($attributeName) || $value === null) {
                return false;
            }
        }
        
        return true;
    }

    /**
     * Formata uma data para o formato ISO 8601
     */
    protected function formatDate(\DateTime $date): string
    {
        return $date->format('Y-m-d\TH:i:s.u\Z');
    }

    /**
     * Converte uma string ISO 8601 para DateTime
     */
    protected function parseDate(string $dateString): \DateTime
    {
        return new \DateTime($dateString);
    }

    /**
     * Gera um UUID v4
     */
    protected function generateUuid(): string
    {
        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0x0fff) | 0x4000,
            mt_rand(0, 0x3fff) | 0x8000,
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff)
        );
    }

    /**
     * Verifica se um valor é um timestamp válido
     */
    protected function isValidTimestamp($value): bool
    {
        if (is_numeric($value)) {
            return $value > 0;
        }
        
        if (is_string($value)) {
            return strtotime($value) !== false;
        }
        
        return false;
    }

    /**
     * Converte um timestamp para DateTime
     */
    protected function timestampToDateTime($timestamp): \DateTime
    {
        if (is_numeric($timestamp)) {
            return (new \DateTime())->setTimestamp($timestamp);
        }
        
        if (is_string($timestamp)) {
            return new \DateTime($timestamp);
        }
        
        throw new \InvalidArgumentException('Timestamp inválido');
    }
}