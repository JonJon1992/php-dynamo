<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Examples\User;
use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\Util\DynamoConfig;

echo "=== PHP Dynamo Library v3.0.0 - Exemplo de Uso ===\n\n";

// 1. Configuração
echo "1. Configurando o DynamoManager...\n";

// Para ambiente local (LocalStack)
$config = new DynamoConfig(
    'test',
    'test',
    'us-east-1',
    'http://localhost:4566' // LocalStack endpoint
);

// Para produção AWS
// $config = new DynamoConfig(
//     $_ENV['AWS_ACCESS_KEY_ID'],
//     $_ENV['AWS_SECRET_ACCESS_KEY'],
//     $_ENV['AWS_REGION']
// );

$manager = DynamoManager::create($config);

echo "✓ DynamoManager configurado\n\n";

// 2. Criar tabela (se não existir)
echo "2. Verificando/criando tabela...\n";

$tableName = 'users';
$tableExists = $manager->getDynamo()->tableExists($tableName);

if (!$tableExists) {
    echo "Criando tabela '$tableName'...\n";
    
    $tableSettings = [
        'TableName' => $tableName,
        'KeySchema' => [
            [
                'AttributeName' => 'PK',
                'KeyType' => 'HASH'
            ],
            [
                'AttributeName' => 'SK',
                'KeyType' => 'RANGE'
            ]
        ],
        'AttributeDefinitions' => [
            [
                'AttributeName' => 'PK',
                'AttributeType' => 'S'
            ],
            [
                'AttributeName' => 'SK',
                'AttributeType' => 'S'
            ]
        ],
        'BillingMode' => 'PAY_PER_REQUEST'
    ];
    
    $result = $manager->createTable($tableSettings);
    
    if (isset($result['error'])) {
        echo "Erro ao criar tabela: " . $result['error'] . "\n";
        exit(1);
    }
    
    echo "Aguardando tabela ficar ativa...\n";
    $manager->getDynamo()->waitForTableActive($tableName);
}

echo "✓ Tabela '$tableName' pronta\n\n";

// 3. Criar um usuário
echo "3. Criando um usuário...\n";

$user = new User();
$user->setEmail('john.doe@example.com')
     ->setName('John Doe')
     ->setAge(30)
     ->setActive(true);

echo "✓ Usuário criado: " . $user->getName() . " (" . $user->getEmail() . ")\n\n";

// 4. Salvar o usuário
echo "4. Salvando usuário no DynamoDB...\n";

try {
    $result = $manager->save($user);
    echo "✓ Usuário salvo com sucesso\n";
    echo "  - PK: " . $user->pk() . "\n";
    echo "  - SK: " . $user->sk() . "\n\n";
} catch (Exception $e) {
    echo "Erro ao salvar usuário: " . $e->getMessage() . "\n";
    exit(1);
}

// 5. Buscar o usuário
echo "5. Buscando usuário...\n";

try {
    $foundUser = $manager->find($tableName, $user->getKey(), User::class);
    
    if ($foundUser) {
        echo "✓ Usuário encontrado:\n";
        echo "  - Nome: " . $foundUser->getName() . "\n";
        echo "  - Email: " . $foundUser->getEmail() . "\n";
        echo "  - Idade: " . ($foundUser->getAge() ?? 'N/A') . "\n";
        echo "  - Ativo: " . ($foundUser->isActive() ? 'Sim' : 'Não') . "\n";
        echo "  - Adulto: " . ($foundUser->isAdult() ? 'Sim' : 'Não') . "\n\n";
    } else {
        echo "✗ Usuário não encontrado\n\n";
    }
} catch (Exception $e) {
    echo "Erro ao buscar usuário: " . $e->getMessage() . "\n";
    exit(1);
}

// 6. Criar mais usuários
echo "6. Criando mais usuários...\n";

$users = [
    (new User())->setEmail('jane.smith@example.com')->setName('Jane Smith')->setAge(25),
    (new User())->setEmail('bob.wilson@example.com')->setName('Bob Wilson')->setAge(35),
    (new User())->setEmail('alice.johnson@example.com')->setName('Alice Johnson')->setAge(28)
];

try {
    $result = $manager->insertItems($users);
    echo "✓ " . count($users) . " usuários criados em lote\n\n";
} catch (Exception $e) {
    echo "Erro ao criar usuários em lote: " . $e->getMessage() . "\n";
    exit(1);
}

// 7. Usar Query Builder
echo "7. Usando Query Builder...\n";

$queryBuilder = $manager->getQueryBuilder();
$queryBuilder->setCondition('PK = :pk')
             ->addValue(':pk', 'USER#john.doe@example.com')
             ->setLimit(10);

$result = $queryBuilder->getResult();
echo "✓ Query executada - " . count($result['items']) . " itens encontrados\n\n";

// 8. Usar Repository
echo "8. Usando Repository...\n";

class UserRepository extends \Jonjon\PhpDynamo\Repository\DynamoRepository
{
    public function __construct(DynamoManager $manager)
    {
        parent::__construct(User::class, $manager);
    }
    
    public function findByEmail(string $email): ?User
    {
        return $this->get($email);
    }
    
    public function findActiveUsers(): array
    {
        return $this->scan(
            'active = :active',
            [':active' => true]
        );
    }
}

$userRepository = new UserRepository($manager);

// Buscar usuário por email
$foundUser = $userRepository->findByEmail('john.doe@example.com');
if ($foundUser) {
    echo "✓ Usuário encontrado via repository: " . $foundUser->getName() . "\n";
}

// Buscar usuários ativos
$activeUsers = $userRepository->findActiveUsers();
echo "✓ " . count($activeUsers['items']) . " usuários ativos encontrados\n\n";

// 9. Atualizar usuário
echo "9. Atualizando usuário...\n";

$userToUpdate = $userRepository->findByEmail('john.doe@example.com');
if ($userToUpdate) {
    $userToUpdate->setAge(31)->touch();
    $manager->save($userToUpdate);
    echo "✓ Usuário atualizado - Nova idade: " . $userToUpdate->getAge() . "\n\n";
}

// 10. Transações
echo "10. Usando transações...\n";

$transaction = $manager->getTransaction();

// Criar um novo usuário
$newUser = (new User())->setEmail('transaction@example.com')->setName('Transaction User');

// Atualizar usuário existente
$existingUser = $userRepository->findByEmail('jane.smith@example.com');
if ($existingUser) {
    $existingUser->setActive(false);
}

$transaction->save($newUser);
if ($existingUser) {
    $transaction->save($existingUser);
}

try {
    $result = $transaction->flush();
    echo "✓ Transação executada com sucesso\n\n";
} catch (Exception $e) {
    echo "Erro na transação: " . $e->getMessage() . "\n";
    exit(1);
}

// 11. Converter para diferentes formatos
echo "11. Convertendo dados...\n";

$user = $userRepository->findByEmail('john.doe@example.com');
if ($user) {
    // Para array
    $userArray = $user->toArray();
    echo "✓ Convertido para array: " . count($userArray) . " campos\n";
    
    // Para JSON
    $userJson = $user->toJson();
    echo "✓ Convertido para JSON: " . strlen($userJson) . " caracteres\n\n";
}

// 12. Listar tabelas
echo "12. Listando tabelas...\n";

$tables = $manager->listTables();
echo "✓ Tabelas disponíveis: " . implode(', ', $tables['tables']) . "\n\n";

echo "=== Exemplo concluído com sucesso! ===\n";
echo "A biblioteca PHP Dynamo v3.0.0 está funcionando corretamente.\n"; 