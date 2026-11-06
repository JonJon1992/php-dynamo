<?php

declare(strict_types=1);

namespace Tests\Unit;

use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\Result;
use Examples\User;
use InvalidArgumentException;
use Jonjon\PhpDynamo\Util\Dynamo;
use Jonjon\PhpDynamo\Util\DynamoTransaction;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Support\FakeDynamoDbHandler;

class DynamoTest extends TestCase
{
    private FakeDynamoDbHandler $handler;
    private Dynamo $dynamo;

    protected function setUp(): void
    {
        $this->handler = new FakeDynamoDbHandler();
        $this->dynamo = new Dynamo($this->handler->config());
    }

    public function testBatchWriteSendsItemsBeforeReturning(): void
    {
        $this->handler->withTable(['PK' => 'HASH', 'SK' => 'RANGE']);

        $this->dynamo->batchWriteItems('users', $this->serializedUsers(3));

        $requests = $this->handler->lastCommand('BatchWriteItem')['RequestItems']['users'];
        $this->assertCount(3, $requests);
        $this->assertSame('USER#user0@example.com', $requests[0]['PutRequest']['Item']['PK']['S']);
    }

    public function testBatchWriteSplitsIntoRequestsOf25(): void
    {
        $this->handler->withTable(['PK' => 'HASH', 'SK' => 'RANGE']);

        $this->dynamo->batchWriteItems('users', $this->serializedUsers(30));

        $sizes = array_map(
            fn($command) => count($command['RequestItems']['users']),
            $this->handler->commandsFor('BatchWriteItem')
        );
        $this->assertSame([25, 5], $sizes);
    }

    public function testBatchWriteThrowsWhenDynamoRejectsTheRequest(): void
    {
        $this->handler
            ->withTable(['PK' => 'HASH', 'SK' => 'RANGE'])
            ->on('BatchWriteItem', FakeDynamoDbHandler::awsError('ValidationException'));

        try {
            $this->dynamo->batchWriteItems('users', $this->serializedUsers(2));
            $this->fail('Era esperada uma exceção');
        } catch (RuntimeException $e) {
            $this->assertInstanceOf(DynamoDbException::class, $e->getPrevious());
            $this->assertStringContainsString('users', $e->getMessage());
        }
    }

    public function testBatchWriteUsesCustomErrorCallbackInsteadOfThrowing(): void
    {
        $this->handler
            ->withTable(['PK' => 'HASH', 'SK' => 'RANGE'])
            ->on('BatchWriteItem', FakeDynamoDbHandler::awsError('ValidationException'));
        $errors = [];

        $this->dynamo->batchWriteItems('users', $this->serializedUsers(2), [
            'error' => function ($error) use (&$errors): void {
                $errors[] = $error;
            },
        ]);

        $this->assertCount(1, $errors);
    }

    public function testBatchDeleteSendsKeysBeforeReturning(): void
    {
        $this->handler->withTable(['PK' => 'HASH', 'SK' => 'RANGE']);

        $this->dynamo->batchDeleteItems('users', [['PK' => 'A', 'SK' => '1'], ['PK' => 'B', 'SK' => '2']]);

        $requests = $this->handler->lastCommand('BatchWriteItem')['RequestItems']['users'];
        $this->assertSame(['PK' => ['S' => 'B'], 'SK' => ['S' => '2']], $requests[1]['DeleteRequest']['Key']);
    }

    public function testPutItemKeepsTheOriginalDynamoDbException(): void
    {
        $this->handler
            ->withTable(['PK' => 'HASH', 'SK' => 'RANGE'])
            ->on('PutItem', FakeDynamoDbHandler::awsError('ConditionalCheckFailedException'));

        try {
            $this->dynamo->putItem('users', ['PK' => 'A', 'SK' => '1']);
            $this->fail('Era esperada uma exceção');
        } catch (DynamoDbException $e) {
            $this->assertSame('ConditionalCheckFailedException', $e->getAwsErrorCode());
        }
    }

    public function testPutItemRequiresPk(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->dynamo->putItem('users', ['name' => 'sem chave']);
    }

    public function testKeysAreMappedToTableKeyNamesInEveryKeyOperation(): void
    {
        $this->handler->withTable(['id' => 'HASH', 'sort' => 'RANGE']);
        $key = ['PK' => 'USER#1', 'SK' => 'PROFILE#1'];
        $expectedKey = ['id' => ['S' => 'USER#1'], 'sort' => ['S' => 'PROFILE#1']];

        $this->dynamo->putItem('users', $key + ['name' => 'Ana']);
        $this->dynamo->getItem('users', $key);
        $this->dynamo->updateItem('users', $key, '#n = :n', [':n' => 'Bia'], null, ['#n' => 'name']);
        $this->dynamo->deleteItem('users', $key);
        $this->dynamo->batchDeleteItems('users', [$key]);

        $this->assertEquals($expectedKey + ['name' => ['S' => 'Ana']], $this->handler->lastCommand('PutItem')['Item']);
        $this->assertSame($expectedKey, $this->handler->lastCommand('GetItem')['Key']);
        $this->assertSame($expectedKey, $this->handler->lastCommand('UpdateItem')['Key']);
        $this->assertSame($expectedKey, $this->handler->lastCommand('DeleteItem')['Key']);
        $this->assertSame(
            $expectedKey,
            $this->handler->lastCommand('BatchWriteItem')['RequestItems']['users'][0]['DeleteRequest']['Key']
        );
        $this->assertCount(1, $this->handler->commandsFor('DescribeTable'), 'O schema deve ficar em cache');
    }

    public function testTransactionOperationsAreMappedToo(): void
    {
        $this->handler->withTable(['id' => 'HASH', 'sort' => 'RANGE']);
        $user = new User(['email' => 'ana@example.com', 'name' => 'Ana']);

        $operations = (new DynamoTransaction($this->dynamo))
            ->save($user)
            ->deleteItem($user)
            ->update('users', $user->getKey(), 'age = :a', [':a' => 30])
            ->getOperations();

        $this->assertSame('USER#ana@example.com', $operations[0]['Put']['Item']['id']['S']);
        $this->assertArrayNotHasKey('PK', $operations[0]['Put']['Item']);
        $this->assertSame('PROFILE#ana@example.com', $operations[1]['Delete']['Key']['sort']['S']);
        $this->assertSame('USER#ana@example.com', $operations[2]['Update']['Key']['id']['S']);
    }

    public function testTableWithPkHashKeyAndOtherSortKeyKeepsPk(): void
    {
        $this->handler->withTable(['PK' => 'HASH', 'createdAt' => 'RANGE']);

        $this->dynamo->putItem('events', ['PK' => 'A', 'SK' => '2026-01-01']);

        $this->assertSame(
            ['PK' => ['S' => 'A'], 'createdAt' => ['S' => '2026-01-01']],
            $this->handler->lastCommand('PutItem')['Item']
        );
    }

    public function testWithoutDescribeTablePermissionKeysAreSentAsIs(): void
    {
        $this->handler->on('DescribeTable', FakeDynamoDbHandler::awsError('AccessDeniedException'));

        $this->dynamo->putItem('users', ['PK' => 'A', 'SK' => '1']);
        $this->dynamo->getItem('users', ['PK' => 'A', 'SK' => '1']);

        $this->assertSame(['PK' => ['S' => 'A'], 'SK' => ['S' => '1']], $this->handler->lastCommand('GetItem')['Key']);
        $this->assertCount(1, $this->handler->commandsFor('DescribeTable'));
    }

    public function testUpdateExpressionDefaultsToSetButAcceptsOtherClauses(): void
    {
        $this->handler->withTable(['PK' => 'HASH']);

        $this->dynamo->updateItem('users', ['PK' => 'A'], 'age = :a', [':a' => 1]);
        $set = $this->handler->lastCommand('UpdateItem');

        $this->dynamo->updateItem('users', ['PK' => 'A'], 'REMOVE age', []);
        $remove = $this->handler->lastCommand('UpdateItem');

        $this->assertSame('SET age = :a', $set['UpdateExpression']);
        $this->assertSame('REMOVE age', $remove['UpdateExpression']);
        $this->assertArrayNotHasKey('ExpressionAttributeValues', $remove->toArray());
    }

    public function testGetItemReturnsNullWhenNotFound(): void
    {
        $this->handler->withTable(['PK' => 'HASH']);

        $this->assertNull($this->dynamo->getItem('users', ['PK' => 'missing']));
    }

    public function testGetItemUnmarshalsTheItem(): void
    {
        $this->handler
            ->withTable(['PK' => 'HASH'])
            ->on('GetItem', new Result(['Item' => ['PK' => ['S' => 'A'], 'age' => ['N' => '30']]]));

        $this->assertSame(['PK' => 'A', 'age' => 30], $this->dynamo->getItem('users', ['PK' => 'A']));
    }

    /**
     * @return list<\stdClass>
     */
    private function serializedUsers(int $count): array
    {
        $users = [];
        for ($i = 0; $i < $count; $i++) {
            $users[] = (new User(['email' => "user$i@example.com", 'name' => "User $i"]))->serialize();
        }

        return $users;
    }
}
