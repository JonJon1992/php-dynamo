<?php

declare(strict_types=1);

namespace Examples;

use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\Repository\DynamoRepository;

class UserRepository extends DynamoRepository
{
    public function __construct(DynamoManager $manager)
    {
        parent::__construct(User::class, $manager);
    }

    public function findByEmail(string $email): ?User
    {
        return $this->get("USER#$email", "PROFILE#$email");
    }

    public function findActiveUsers(): array
    {
        return $this->scan('active = :active', [':active' => true]);
    }
}
