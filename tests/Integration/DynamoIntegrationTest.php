<?php

declare(strict_types=1);

namespace Tests\Integration;

use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\Util\DynamoConfig;
use PHPUnit\Framework\TestCase;
use Tests\Support\Note;

/**
 * Roda contra um DynamoDB real (DynamoDB Local, LocalStack ou AWS).
 *
 *   docker compose up -d
 *   DYNAMODB_ENDPOINT=http://localhost:8000 composer test:integration
 */
class DynamoIntegrationTest extends TestCase
{
    private static ?DynamoManager $manager = null;
    private static string $pkSkTable;
    private static string $customKeysTable;

    public static function setUpBeforeClass(): void
    {
        $endpoint = getenv('DYNAMODB_ENDPOINT');
        if (!is_string($endpoint) || $endpoint === '') {
            return;
        }

        self::$manager = DynamoManager::create(DynamoConfig::local($endpoint));

        $suffix = bin2hex(random_bytes(4));
        self::$pkSkTable = "php_dynamo_it_$suffix";
        self::$customKeysTable = "php_dynamo_it_custom_$suffix";

        self::createTable(self::$pkSkTable, 'PK', 'SK');
        self::createTable(self::$customKeysTable, 'id', 'sort');
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$manager === null) {
            return;
        }

        self::$manager->deleteTable(self::$pkSkTable);
        self::$manager->deleteTable(self::$customKeysTable);
        self::$manager = null;
    }

    protected function setUp(): void
    {
        if (self::$manager === null) {
            $this->markTestSkipped('Defina DYNAMODB_ENDPOINT (ex.: http://localhost:8000) para rodar a integração.');
        }

        Note::$table = self::$pkSkTable;
    }

    public function testSaveFindUpdateQueryAndDelete(): void
    {
        $manager = $this->manager();
        $note = Note::make('ana', 'first', 'olá');

        $manager->save($note);

        $found = $manager->find(self::$pkSkTable, $note->getKey(), Note::class);
        $this->assertInstanceOf(Note::class, $found);
        $this->assertSame('olá', $found->getText());

        $manager->update(self::$pkSkTable, $note->getKey(), '#t = :t', [':t' => 'editado'], null, ['#t' => 'text']);
        $updated = $manager->find(self::$pkSkTable, $note->getKey(), Note::class);
        $this->assertInstanceOf(Note::class, $updated);
        $this->assertSame('editado', $updated->getText());

        $items = $manager->getQueryBuilder(Note::class)
            ->setCondition('PK = :pk')
            ->addValue(':pk', 'OWNER#ana')
            ->getItemsResult();
        $this->assertCount(1, $items);

        $manager->delete($note);
        $this->assertNull($manager->find(self::$pkSkTable, $note->getKey(), Note::class));
    }

    public function testBatchInsertAndDeleteWriteEveryItem(): void
    {
        $notes = [];
        for ($i = 0; $i < 30; $i++) {
            $notes[] = Note::make('batch', "n$i", "nota $i");
        }

        $this->manager()->insertItems($notes);
        $this->assertSame(30, $this->countNotes('batch'));

        $this->manager()->deleteItems($notes);
        $this->assertSame(0, $this->countNotes('batch'));
    }

    public function testTransactionWritesAllOperations(): void
    {
        $manager = $this->manager();

        $manager->getTransaction()
            ->save(Note::make('tx', 'a'))
            ->save(Note::make('tx', 'b'))
            ->flush();

        $this->assertSame(2, $this->countNotes('tx'));
    }

    public function testTableWithCustomKeyNamesSupportsTheWholeLifecycle(): void
    {
        Note::$table = self::$customKeysTable;
        $manager = $this->manager();
        $note = Note::make('bia', 'mapped', 'chaves id/sort');

        $manager->save($note);

        $raw = $manager->getDynamo()->getItem(self::$customKeysTable, $note->getKey());
        $this->assertSame('OWNER#bia', $raw['id'] ?? null);
        $this->assertArrayNotHasKey('PK', $raw);
        $found = $manager->find(self::$customKeysTable, $note->getKey(), Note::class);
        $this->assertInstanceOf(Note::class, $found);
        $this->assertSame('chaves id/sort', $found->getText());

        $manager->delete($note);
        $this->assertNull($manager->getDynamo()->getItem(self::$customKeysTable, $note->getKey()));

        $manager->insertItems([Note::make('bia', 'x'), Note::make('bia', 'y')]);
        $count = $manager->getDynamo()->query(
            self::$customKeysTable,
            'id = :pk',
            [':pk' => 'OWNER#bia'],
            params: ['Select' => 'COUNT']
        )['Count'];
        $this->assertSame(2, $count);
    }

    private function countNotes(string $owner): int
    {
        return $this->manager()->getDynamo()->query(
            self::$pkSkTable,
            'PK = :pk',
            [':pk' => "OWNER#$owner"],
            params: ['Select' => 'COUNT']
        )['Count'];
    }

    private function manager(): DynamoManager
    {
        return self::$manager ?? throw new \LogicException('Manager não inicializado.');
    }

    private static function createTable(string $name, string $hashKey, string $rangeKey): void
    {
        self::$manager?->createTable([
            'TableName' => $name,
            'KeySchema' => [
                ['AttributeName' => $hashKey, 'KeyType' => 'HASH'],
                ['AttributeName' => $rangeKey, 'KeyType' => 'RANGE'],
            ],
            'AttributeDefinitions' => [
                ['AttributeName' => $hashKey, 'AttributeType' => 'S'],
                ['AttributeName' => $rangeKey, 'AttributeType' => 'S'],
            ],
            'BillingMode' => 'PAY_PER_REQUEST',
        ]);

        self::$manager?->getDynamo()->waitForTableActive($name);
    }
}
