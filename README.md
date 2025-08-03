# PHP Dynamo v3.0.0

Uma biblioteca PHP moderna e limpa para trabalhar com Amazon DynamoDB, com suporte automático para ambiente local (LocalStack) e produção (AWS).

## 🚀 Características

- **PHP 8.1+** - Suporte completo às funcionalidades modernas do PHP
- **AWS SDK v3.350** - Versão mais recente e estável
- **LocalStack** - Desenvolvimento local sem custos
- **Type Safety** - Strict types e type hints em todo o código
- **Query Builder** - Interface fluente para queries complexas
- **Repository Pattern** - Abstração de dados limpa e organizada
- **Transações** - Suporte completo a transações DynamoDB
- **PHP 8 Attributes** - Anotações modernas para mapeamento de propriedades
- **Testes** - Cobertura completa com PHPUnit
- **Análise Estática** - PHPStan para qualidade de código
- **Padrões de Código** - PHP CodeSniffer para consistência

## 📦 Instalação

```bash
composer require jonjon1992/php-dynamo
```

## 🔧 Configuração

### Ambiente Local (LocalStack)

```php
use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\Util\DynamoConfig;

$config = new DynamoConfig(
    'test',
    'test', 
    'us-east-1',
    'http://localhost:4566' // LocalStack endpoint
);

$manager = DynamoManager::create($config);
```

### Ambiente de Produção (AWS)

```php
$config = new DynamoConfig(
    $_ENV['AWS_ACCESS_KEY_ID'],
    $_ENV['AWS_SECRET_ACCESS_KEY'],
    $_ENV['AWS_REGION']
);

$manager = DynamoManager::create($config);
```

## 📝 Exemplo de Uso

### 1. Definindo um Item

```php
<?php

use Jonjon\PhpDynamo\Item\DynamoItem;
use Jonjon\PhpDynamo\Item\DynamoProperty;

class User extends DynamoItem
{
    #[DynamoProperty(['name' => 'email', 'type' => 'string', 'required' => true])]
    private string $email;

    #[DynamoProperty(['name' => 'name', 'type' => 'string', 'required' => true])]
    private string $name;

    #[DynamoProperty(['name' => 'age', 'type' => 'integer', 'required' => false])]
    private ?int $age = null;

    public function pk(): string
    {
        return "USER#{$this->email}";
    }

    public function sk(): ?string
    {
        return "PROFILE#{$this->email}";
    }

    public static function getTableName(): string
    {
        return 'users';
    }

    // Getters e Setters...
}
```

### 2. Operações Básicas

```php
// Criar e salvar
$user = new User();
$user->setEmail('john@example.com')->setName('John Doe');
$manager->save($user);

// Buscar
$foundUser = $manager->find('users', $user->getKey(), User::class);

// Atualizar
$user->setAge(30);
$manager->save($user);

// Remover
$manager->delete($user);
```

### 3. Query Builder

```php
$queryBuilder = $manager->getQueryBuilder();
$result = $queryBuilder
    ->setCondition('PK = :pk')
    ->addValue(':pk', 'USER#john@example.com')
    ->setFilter('age > :age')
    ->addValue(':age', 25)
    ->setLimit(10)
    ->setOrderByDesc()
    ->getResult();
```

### 4. Repository Pattern

```php
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
        return $this->scan('active = :active', [':active' => true]);
    }
}

$repository = new UserRepository($manager);
$user = $repository->findByEmail('john@example.com');
```

### 5. Transações

```php
$transaction = $manager->getTransaction();

$transaction->save($user1);
$transaction->save($user2);
$transaction->delete($oldUser);

$result = $transaction->flush();
```

### 6. Operações em Lote

```php
// Inserir múltiplos itens
$users = [new User(), new User(), new User()];
$manager->insertItems($users);

// Remover múltiplos itens
$manager->deleteItems($users);
```

## 🧪 Testes

```bash
# Executar todos os testes
composer test

# Executar testes unitários
composer test:unit

# Executar testes de integração
composer test:integration

# Gerar relatório de cobertura
composer test:coverage
```

## 🔍 Análise de Código

```bash
# Análise estática com PHPStan
composer phpstan

# Verificar padrões de código
composer cs

# Corrigir padrões de código automaticamente
composer cs:fix

# Executar todas as verificações
composer check
```

## 📚 Documentação

```bash
# Gerar documentação
composer docs
```

## 🛠️ Desenvolvimento

### Estrutura do Projeto

```
lib/
├── DynamoManager.php          # Classe principal
├── DynamoQueryBuilder.php     # Query Builder
├── Item/
│   ├── DynamoItem.php         # Classe base para itens
│   └── DynamoProperty.php     # Atributos PHP 8
├── Repository/
│   └── DynamoRepository.php   # Padrão Repository
└── Util/
    ├── Dynamo.php             # Cliente DynamoDB
    ├── DynamoConfig.php       # Configuração
    ├── DynamoTransaction.php  # Transações
    └── DynamoUtil.php         # Utilitários
```

### Scripts Disponíveis

- `composer test` - Executar testes
- `composer phpstan` - Análise estática
- `composer cs` - Verificar padrões de código
- `composer cs:fix` - Corrigir padrões de código
- `composer docs` - Gerar documentação
- `composer check` - Executar todas as verificações
- `composer example` - Executar exemplo de uso

## 🔄 Migração da v2.x

### Principais Mudanças

1. **PHP 8.1+** - Requerido em vez de PHP 8.0
2. **AWS SDK 3.350** - Atualizado da versão 3.324
3. **Strict Types** - Adicionado em todos os arquivos
4. **Type Hints** - Melhorados em todos os métodos
5. **Match Expression** - Substituído switch statements
6. **Métodos Adicionais** - Novos métodos utilitários
7. **Melhor Tratamento de Erros** - Exceções mais específicas

### Código Antigo vs Novo

```php
// v2.x
$manager = DynamoManager::createWithCredentials($key, $secret, $region);

// v3.0
$config = new DynamoConfig($key, $secret, $region);
$manager = DynamoManager::create($config);

// v2.x
switch ($hydrate) {
    case self::HYDRATE_OBJECT:
        // ...
}

// v3.0
return match ($hydrate) {
    self::HYDRATE_OBJECT => $this->convertToObject($item, $class),
    self::HYDRATE_ARRAY => json_decode(json_encode($item, JSON_UNESCAPED_SLASHES), true),
    default => $item,
};
```

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🆘 Suporte

- **Issues**: [GitHub Issues](https://github.com/jonjon/php-dynamo/issues)
- **Documentação**: [GitHub Wiki](https://github.com/jonjon/php-dynamo/wiki)
- **Email**: jonathan201392127@gmail.com

## 🙏 Agradecimentos

- AWS SDK for PHP
- LocalStack
- PHPUnit
- PHPStan
- PHP CodeSniffer

---

**Desenvolvido com ❤️ por Jonathan Braga** 