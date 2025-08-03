<?php

declare(strict_types=1);

namespace Jonjon\PhpDynamo\Util;

class DynamoConfig
{
    public function __construct(
        protected readonly string $key = 'test',
        protected readonly string $secret = 'test',
        protected readonly string $region = 'us-east-1',
        protected readonly ?string $endpoint = null
    ) {
    }

    public function getClientParams(): array
    {
        $params = [
            'credentials' => [
                'key' => $this->key,
                'secret' => $this->secret
            ],
            'region' => $this->region,
            'version' => 'latest'
        ];

        if ($this->endpoint) {
            $params['endpoint'] = $this->endpoint;
        }

        return $params;
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getSecret(): string
    {
        return $this->secret;
    }

    public function getRegion(): string
    {
        return $this->region;
    }

    public function getEndpoint(): ?string
    {
        return $this->endpoint;
    }
} 