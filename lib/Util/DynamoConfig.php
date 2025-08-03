<?php

namespace Jonjon\PhpDynamo\Util;

class DynamoConfig
{
    protected string $key;
    protected string $secret;
    protected string $region;
    protected ?string $endpoint;

    public function __construct(
        string $key = 'test',
        string $secret = 'test',
        string $region = 'us-east-1',
        ?string $endpoint = null
    ) {
        $this->key = $key;
        $this->secret = $secret;
        $this->region = $region;
        $this->endpoint = $endpoint;
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