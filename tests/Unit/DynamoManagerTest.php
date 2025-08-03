<?php

declare(strict_types=1);

namespace Tests\Unit;

use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\Util\DynamoConfig;
use PHPUnit\Framework\TestCase;

class DynamoManagerTest extends TestCase
{
    private DynamoManager $manager;

    protected function setUp(): void
    {
        $config = new DynamoConfig('test', 'test', 'us-east-1');
        $this->manager = DynamoManager::create($config);
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

    public function testGetDynamo(): void
    {
        $dynamo = $this->manager->getDynamo();
        
        $this->assertNotNull($dynamo);
    }

    public function testGetTransaction(): void
    {
        $transaction = $this->manager->getTransaction();
        
        $this->assertNotNull($transaction);
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
        
        $this->assertIsObject($result);
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
        
        $this->assertIsArray($result);
        $this->assertCount(2, $result);
        $this->assertEquals('John', $result[0]['name']);
        $this->assertEquals('Jane', $result[1]['name']);
    }

    public function testListTables(): void
    {
        $result = $this->manager->listTables();
        
        $this->assertIsArray($result);
        $this->assertArrayHasKey('tables', $result);
    }
} 