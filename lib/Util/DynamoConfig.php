<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Util;

use InvalidArgumentException;

class DynamoConfig
{
    /**
     * DynamoDB Local do docker-compose.yml. Para LocalStack, use http://localhost:4566.
     */
    public const LOCAL_ENDPOINT = 'http://localhost:8000';

    /**
     * @param array<string, mixed> $clientOptions Opções extras repassadas ao DynamoDbClient
     *                                            (ex.: retries, http, handler). Têm precedência.
     */
    public function __construct(
        protected readonly ?string $key = null,
        protected readonly ?string $secret = null,
        protected readonly string $region = 'us-east-1',
        protected readonly ?string $endpoint = null,
        protected readonly ?string $sessionToken = null,
        protected readonly array $clientOptions = []
    ) {
        if (($key === null) !== ($secret === null)) {
            throw new InvalidArgumentException(
                'Informe key e secret juntos, ou nenhum dos dois para usar as credenciais padrão da AWS.'
            );
        }

        if ($sessionToken !== null && $key === null) {
            throw new InvalidArgumentException('O sessionToken exige key e secret.');
        }
    }

    /**
     * Configuração para desenvolvimento local (LocalStack ou DynamoDB Local).
     */
    public static function local(string $endpoint = self::LOCAL_ENDPOINT, string $region = 'us-east-1'): self
    {
        return new self('test', 'test', $region, $endpoint);
    }

    public static function fromEnvironment(): self
    {
        $endpoint = self::env('DYNAMODB_ENDPOINT') ?? self::env('AWS_ENDPOINT_URL_DYNAMODB');
        $region = self::env('AWS_REGION') ?? self::env('AWS_DEFAULT_REGION') ?? 'us-east-1';

        if ($endpoint !== null && self::env('AWS_ACCESS_KEY_ID') === null) {
            return self::local($endpoint, $region);
        }

        return new self(region: $region, endpoint: $endpoint);
    }

    /**
     * @return array<string, mixed>
     */
    public function getClientParams(): array
    {
        $params = [
            'region' => $this->region,
            'version' => 'latest',
        ];

        if ($this->key !== null && $this->secret !== null) {
            $params['credentials'] = [
                'key' => $this->key,
                'secret' => $this->secret,
            ];

            if ($this->sessionToken !== null) {
                $params['credentials']['token'] = $this->sessionToken;
            }
        }

        if ($this->endpoint) {
            $params['endpoint'] = $this->endpoint;
        }

        return array_replace($params, $this->clientOptions);
    }

    public function getKey(): ?string
    {
        return $this->key;
    }

    public function getSecret(): ?string
    {
        return $this->secret;
    }

    public function getSessionToken(): ?string
    {
        return $this->sessionToken;
    }

    public function getRegion(): string
    {
        return $this->region;
    }

    public function getEndpoint(): ?string
    {
        return $this->endpoint;
    }

    /**
     * @return array<string, mixed>
     */
    public function getClientOptions(): array
    {
        return $this->clientOptions;
    }

    private static function env(string $name): ?string
    {
        $value = $_ENV[$name] ?? $_SERVER[$name] ?? getenv($name);

        return is_string($value) && $value !== '' ? $value : null;
    }
}
