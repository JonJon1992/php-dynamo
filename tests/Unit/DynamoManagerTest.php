<?php

declare(strict_types=1);

namespace Tests\Unit;

use Aws\Result;
use Examples\User;
use InvalidArgumentException;
use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\DynamoQueryBuilder;
use Jonjon\PhpDynamo\Util\DynamoConfig;
use PHPUnit\Framework\TestCase;
use Tests\Support\FakeDynamoDbHandler;

class DynamoManagerTest extends TestCase
{
    private FakeDynamoDbHandler $handler;
    private DynamoManager $manager;

    protected function setUp(): void
    {
        $this->handler = (new FakeDynamoDbHandler())->withTable(['PK' => 'HASH', 'SK' => 'RANGE']);
        $this->manager = DynamoManager::create($this->handler->config());
    }

    public function testCreateWithConfig(): void
    {
        $config = new DynamoConfig('test', 'test', 'us-east-1');
        $manager = DynamoManager::create($config);

        $this->assertInstanceOf(DynamoManager::class, $manager);
    }

    public function testCreateWithCredentials(): void
    {
        $manager = DynamoManager::createWithCredentials('test', 'test', 'us-east-1');

        $this->assertInstanceOf(DynamoManager::class, $manager);
    }

    public function testGetDynamoUsesTheGivenConfig(): void
    {
        $this->assertSame('us-east-1', $this->manager->getDynamo()->getConfig()->getRegion());
    }

    public function testGetTransactionStartsEmpty(): void
    {
        $this->assertTrue($this->manager->getTransaction()->isEmpty());
    }

    public function testGetQueryBuilder(): void
    {
        $builder = $this->manager->getQueryBuilder(User::class);

        $this->assertInstanceOf(DynamoQueryBuilder::class, $builder);
    }

    public function testConvertToArray(): void
    {
        $data = ['name' => 'John', 'age' => 30];
        $result = $this->manager->convert($data, DynamoManager::HYDRATE_ARRAY);

        $this->assertIsArray($result);
        $this->assertEquals('John', $result['name']);
        $this->assertEquals(30, $result['age']);
    }

    public function testConvertToStdClass(): void
    {
        $data = ['name' => 'John', 'age' => 30];
        $result = $this->manager->convert($data, DynamoManager::HYDRATE_STDCLASS);

        $this->assertInstanceOf(\stdClass::class, $result);
        $this->assertEquals('John', $result->name);
        $this->assertEquals(30, $result->age);
    }

    public function testConvertItems(): void
    {
        $items = [
            ['name' => 'John', 'age' => 30],
            ['name' => 'Jane', 'age' => 25]
        ];

        $result = $this->manager->convertItems($items, DynamoManager::HYDRATE_ARRAY);

        $this->assertCount(2, $result);
        $this->assertEquals('John', $result[0]['name']);
        $this->assertEquals('Jane', $result[1]['name']);
    }

    public function testListTables(): void
    {
        $this->handler->on('ListTables', new Result(['TableNames' => ['users']]));

        $result = $this->manager->listTables();

        $this->assertSame(['tables' => ['users']], $result);
    }

    public function testInsertItemsWritesEverythingBeforeReturning(): void
    {
        $this->manager->insertItems([$this->user('a'), $this->user('b')]);

        $requests = $this->handler->lastCommand('BatchWriteItem')['RequestItems']['users'];
        $this->assertCount(2, $requests);
    }

    public function testInsertItemsAcceptsArraysWithoutSequentialKeys(): void
    {
        $this->manager->insertItems(['first' => $this->user('a'), 'second' => $this->user('b')]);

        $this->assertCount(2, $this->handler->lastCommand('BatchWriteItem')['RequestItems']['users']);
    }

    public function testDeleteItemsSendsTheKeys(): void
    {
        $this->manager->deleteItems([$this->user('a')]);

        $request = $this->handler->lastCommand('BatchWriteItem')['RequestItems']['users'][0];
        $this->assertSame(
            ['PK' => ['S' => 'USER#a@example.com'], 'SK' => ['S' => 'PROFILE#a@example.com']],
            $request['DeleteRequest']['Key']
        );
    }

    public function testInsertItemsRejectsObjectsThatAreNotItems(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @phpstan-ignore argument.type (entrada inválida de propósito)
        $this->manager->insertItems([$this->user('a'), new \stdClass()]);
    }

    public function testInsertItemsWithEmptyListDoesNothing(): void
    {
        $this->assertNull($this->manager->insertItems([]));
        $this->assertSame([], $this->handler->commandsFor('BatchWriteItem'));
    }

    private function user(string $name): User
    {
        return new User(['email' => "$name@example.com", 'name' => $name]);
    }
}
