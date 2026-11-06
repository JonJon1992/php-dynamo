<?php

declare(strict_types=1);

namespace Tests\Unit;

use InvalidArgumentException;
use Jonjon\PhpDynamo\Util\DynamoConfig;
use PHPUnit\Framework\TestCase;

class DynamoConfigTest extends TestCase
{
    private const ENV_VARS = [
        'DYNAMODB_ENDPOINT',
        'AWS_ENDPOINT_URL_DYNAMODB',
        'AWS_REGION',
        'AWS_DEFAULT_REGION',
        'AWS_ACCESS_KEY_ID',
    ];

    /** @var array<string, array{env: mixed, server: mixed, getenv: string|false}> */
    private array $originalEnv = [];

    protected function setUp(): void
    {
        foreach (self::ENV_VARS as $name) {
            $this->originalEnv[$name] = [
                'env' => $_ENV[$name] ?? null,
                'server' => $_SERVER[$name] ?? null,
                'getenv' => getenv($name),
            ];
            $this->setEnv($name, null);
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->originalEnv as $name => $original) {
            unset($_ENV[$name], $_SERVER[$name]);
            if ($original['env'] !== null) {
                $_ENV[$name] = $original['env'];
            }
            if ($original['server'] !== null) {
                $_SERVER[$name] = $original['server'];
            }
            putenv($original['getenv'] === false ? $name : "$name={$original['getenv']}");
        }
    }

    public function testWithoutCredentialsLetsTheSdkUseTheDefaultProviderChain(): void
    {
        $params = (new DynamoConfig(region: 'sa-east-1'))->getClientParams();

        $this->assertArrayNotHasKey('credentials', $params);
        $this->assertSame('sa-east-1', $params['region']);
        $this->assertArrayNotHasKey('endpoint', $params);
    }

    public function testExplicitCredentialsIncludeSessionToken(): void
    {
        $params = (new DynamoConfig('key', 'secret', 'us-east-1', null, 'token'))->getClientParams();

        $this->assertSame(['key' => 'key', 'secret' => 'secret', 'token' => 'token'], $params['credentials']);
    }

    public function testKeyWithoutSecretIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new DynamoConfig('key');
    }

    public function testSessionTokenWithoutKeyIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new DynamoConfig(sessionToken: 'token');
    }

    public function testLocalUsesFakeCredentialsAndLocalEndpoint(): void
    {
        $params = DynamoConfig::local()->getClientParams();

        $this->assertSame(['key' => 'test', 'secret' => 'test'], $params['credentials']);
        $this->assertSame('http://localhost:8000', $params['endpoint']);
    }

    public function testClientOptionsAreMergedWithPrecedence(): void
    {
        $config = new DynamoConfig(region: 'us-east-1', clientOptions: ['region' => 'eu-west-1', 'retries' => 5]);

        $params = $config->getClientParams();

        $this->assertSame('eu-west-1', $params['region']);
        $this->assertSame(5, $params['retries']);
    }

    public function testFromEnvironmentWithEndpointAndNoKeysUsesLocalCredentials(): void
    {
        $this->setEnv('DYNAMODB_ENDPOINT', 'http://dynamodb:8000');
        $this->setEnv('AWS_REGION', 'sa-east-1');

        $config = DynamoConfig::fromEnvironment();

        $this->assertSame('http://dynamodb:8000', $config->getEndpoint());
        $this->assertSame('sa-east-1', $config->getRegion());
        $this->assertSame('test', $config->getKey());
    }

    public function testFromEnvironmentInProductionLeavesCredentialsToTheSdk(): void
    {
        $this->setEnv('AWS_DEFAULT_REGION', 'eu-central-1');

        $config = DynamoConfig::fromEnvironment();

        $this->assertNull($config->getEndpoint());
        $this->assertNull($config->getKey());
        $this->assertSame('eu-central-1', $config->getRegion());
        $this->assertArrayNotHasKey('credentials', $config->getClientParams());
    }

    private function setEnv(string $name, ?string $value): void
    {
        unset($_ENV[$name], $_SERVER[$name]);

        if ($value === null) {
            putenv($name);
            return;
        }

        $_ENV[$name] = $value;
        putenv("$name=$value");
    }
}
