<?php

declare(strict_types=1);

namespace Tests\Unit;

use Examples\User;
use PHPUnit\Framework\TestCase;

class DynamoItemTest extends TestCase
{
    public function testCreateUserWithData(): void
    {
        $data = [
            'email' => 'test@example.com',
            'name' => 'Test User',
            'age' => 25,
            'active' => true
        ];

        $user = new User($data);

        $this->assertEquals('test@example.com', $user->getEmail());
        $this->assertEquals('Test User', $user->getName());
        $this->assertEquals(25, $user->getAge());
        $this->assertTrue($user->isActive());
    }

    public function testCreateUserWithoutData(): void
    {
        $user = new User();

        $this->assertNotNull($user->getCreatedAt());
        $this->assertNotNull($user->getUpdatedAt());
    }

    public function testUserPk(): void
    {
        $user = new User(['email' => 'test@example.com', 'name' => 'Test User']);
        
        $this->assertEquals('USER#test@example.com', $user->pk());
    }

    public function testUserSk(): void
    {
        $user = new User(['email' => 'test@example.com', 'name' => 'Test User']);
        
        $this->assertEquals('PROFILE#test@example.com', $user->sk());
    }

    public function testUserGetKey(): void
    {
        $user = new User(['email' => 'test@example.com', 'name' => 'Test User']);
        $key = $user->getKey();
        
        $this->assertArrayHasKey('PK', $key);
        $this->assertArrayHasKey('SK', $key);
        $this->assertEquals('USER#test@example.com', $key['PK']);
        $this->assertEquals('PROFILE#test@example.com', $key['SK']);
    }

    public function testUserItemType(): void
    {
        $user = new User();
        
        $this->assertEquals('User', $user->itemType());
    }

    public function testUserTableName(): void
    {
        $this->assertEquals('users', User::getTableName());
    }

    public function testUserSerialize(): void
    {
        $user = new User([
            'email' => 'test@example.com',
            'name' => 'Test User',
            'age' => 25
        ]);

        $serialized = $user->serialize();

        $this->assertObjectHasAttribute('PK', $serialized);
        $this->assertObjectHasAttribute('SK', $serialized);
        $this->assertObjectHasAttribute('email', $serialized);
        $this->assertObjectHasAttribute('name', $serialized);
        $this->assertObjectHasAttribute('age', $serialized);
        $this->assertObjectHasAttribute('ItemType', $serialized);
    }

    public function testUserToArray(): void
    {
        $user = new User([
            'email' => 'test@example.com',
            'name' => 'Test User'
        ]);

        $array = $user->toArray();

        $this->assertIsArray($array);
        $this->assertArrayHasKey('PK', $array);
        $this->assertArrayHasKey('email', $array);
        $this->assertArrayHasKey('name', $array);
    }

    public function testUserToJson(): void
    {
        $user = new User([
            'email' => 'test@example.com',
            'name' => 'Test User'
        ]);

        $json = $user->toJson();

        $this->assertIsString($json);
        $this->assertNotEmpty($json);
        
        $decoded = json_decode($json, true);
        $this->assertIsArray($decoded);
        $this->assertArrayHasKey('email', $decoded);
    }

    public function testUserFromArray(): void
    {
        $data = [
            'email' => 'test@example.com',
            'name' => 'Test User'
        ];

        $user = User::fromArray($data);

        $this->assertInstanceOf(User::class, $user);
        $this->assertEquals('test@example.com', $user->getEmail());
        $this->assertEquals('Test User', $user->getName());
    }

    public function testUserFromJson(): void
    {
        $json = '{"email":"test@example.com","name":"Test User"}';

        $user = User::fromJson($json);

        $this->assertInstanceOf(User::class, $user);
        $this->assertEquals('test@example.com', $user->getEmail());
        $this->assertEquals('Test User', $user->getName());
    }

    public function testUserTouch(): void
    {
        $user = new User(['email' => 'test@example.com', 'name' => 'Test User']);
        $originalUpdatedAt = $user->getUpdatedAt();
        
        sleep(1); // Garantir que o timestamp será diferente
        $user->touch();
        
        $this->assertNotEquals($originalUpdatedAt, $user->getUpdatedAt());
    }

    public function testUserIsAdult(): void
    {
        $adultUser = new User(['email' => 'adult@example.com', 'name' => 'Adult', 'age' => 25]);
        $minorUser = new User(['email' => 'minor@example.com', 'name' => 'Minor', 'age' => 16]);
        $unknownAgeUser = new User(['email' => 'unknown@example.com', 'name' => 'Unknown']);

        $this->assertTrue($adultUser->isAdult());
        $this->assertFalse($minorUser->isAdult());
        $this->assertFalse($unknownAgeUser->isAdult());
    }

    public function testUserGetBasicInfo(): void
    {
        $user = new User([
            'email' => 'test@example.com',
            'name' => 'Test User',
            'age' => 25,
            'active' => true
        ]);

        $basicInfo = $user->getBasicInfo();

        $this->assertArrayHasKey('email', $basicInfo);
        $this->assertArrayHasKey('name', $basicInfo);
        $this->assertArrayHasKey('active', $basicInfo);
        $this->assertArrayNotHasKey('age', $basicInfo);
        $this->assertEquals('test@example.com', $basicInfo['email']);
    }
} 