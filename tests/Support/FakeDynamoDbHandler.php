<?php

declare(strict_types=1);

namespace Tests\Support;

use Aws\CommandInterface;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Result;
use Aws\ResultInterface;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use Jonjon\PhpDynamo\Util\DynamoConfig;
use Psr\Http\Message\RequestInterface;
use Throwable;

/**
 * Handler do AWS SDK que registra os comandos e devolve respostas programadas por operação.
 * Operações sem resposta programada recebem um Result vazio.
 */
final class FakeDynamoDbHandler
{
    /** @var list<CommandInterface> */
    public array $commands = [];

    /** @var array<string, list<ResultInterface|Throwable|callable(CommandInterface): (ResultInterface|Throwable)>> */
    private array $responses = [];

    /**
     * @param ResultInterface|Throwable|callable(CommandInterface): (ResultInterface|Throwable) $response
     */
    public function on(string $operation, ResultInterface|Throwable|callable $response): self
    {
        $this->responses[$operation][] = $response;

        return $this;
    }

    /**
     * Responde DescribeTable com o KeySchema informado (ex.: ['id' => 'HASH', 'sort' => 'RANGE']).
     *
     * @param array<string, string> $keySchema
     */
    public function withTable(array $keySchema): self
    {
        $definitions = [];
        foreach ($keySchema as $name => $type) {
            $definitions[] = ['AttributeName' => $name, 'KeyType' => $type];
        }

        return $this->on('DescribeTable', new Result(['Table' => ['KeySchema' => $definitions]]));
    }

    public function config(): DynamoConfig
    {
        return new DynamoConfig('test', 'test', 'us-east-1', clientOptions: ['handler' => $this]);
    }

    /**
     * @return list<CommandInterface>
     */
    public function commandsFor(string $operation): array
    {
        return array_values(array_filter(
            $this->commands,
            fn(CommandInterface $command) => $command->getName() === $operation
        ));
    }

    public function lastCommand(string $operation): CommandInterface
    {
        $commands = $this->commandsFor($operation);
        if ($commands === []) {
            throw new \LogicException("Nenhum comando $operation foi enviado.");
        }

        return $commands[array_key_last($commands)];
    }

    /**
     * @return callable(CommandInterface): DynamoDbException
     */
    public static function awsError(string $code): callable
    {
        return fn(CommandInterface $command) => new DynamoDbException(
            $code,
            $command,
            ['code' => $code, 'message' => "$code (simulado)"]
        );
    }

    public function __invoke(CommandInterface $command, RequestInterface $request): PromiseInterface
    {
        $this->commands[] = $command;

        $queue = $this->responses[$command->getName()] ?? [];
        $response = $queue === [] ? new Result([]) : array_shift($queue);
        $this->responses[$command->getName()] = $queue;

        if (is_callable($response)) {
            $response = $response($command);
        }

        return $response instanceof Throwable ? Create::rejectionFor($response) : Create::promiseFor($response);
    }
}
