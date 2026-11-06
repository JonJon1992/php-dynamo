# PHP Dynamo

Uma biblioteca PHP moderna e limpa para trabalhar com Amazon DynamoDB, com configuração simples para ambiente local (DynamoDB Local ou LocalStack) e produção (AWS).

## 🚀 Características

- **PHP 8.2+** - Testado até o PHP 8.5
- **AWS SDK v3.386+**
- **Credenciais da AWS** - Usa a cadeia padrão do SDK (IAM Role, variáveis de ambiente, `~/.aws`) ou credenciais explícitas
- **Ambiente local** - DynamoDB Local via `docker compose`, sem custo nem conta
- **Type Safety** - Strict types e type hints em todo o código
- **Query Builder** - Interface fluente para queries
- **Repository Pattern** - Abstração de dados limpa e organizada
- **Transações** - `TransactWriteItems` com até 100 operações
- **Operações em lote** - Envio em blocos de 25, com reenvio de itens não processados e erro explícito em caso de falha
- **PHP 8 Attributes** - Mapeamento de propriedades com `#[DynamoProperty]`
- **Testes** - Unitários (cliente AWS simulado) e de integração (DynamoDB real)
- **Análise Estática** - PHPStan nível 8
- **Padrões de Código** - PSR-12 com PHP CodeSniffer

## 📦 Instalação

```bash
composer require jonjon1992/php-dynamo
```

## 🔧 Configuração

### Ambiente Local

```php
use Jonjon\PhpDynamo\DynamoManager;
use Jonjon\PhpDynamo\Util\DynamoConfig;

// DynamoDB Local do docker-compose.yml (http://localhost:8000)
$manager = DynamoManager::create(DynamoConfig::local());

// LocalStack
$manager = DynamoManager::create(DynamoConfig::local('http://localhost:4566'));
```

### Produção (AWS)

Sem `key`/`secret`, o SDK resolve as credenciais sozinho: variáveis de ambiente, `~/.aws/credentials` ou a IAM Role da instância, do container (ECS/EKS) ou da Lambda.

```php
$manager = DynamoManager::create(new DynamoConfig(region: 'sa-east-1'));
```

Com credenciais explícitas (o `sessionToken` é opcional, para credenciais temporárias do STS):

```php
$config = new DynamoConfig(
    key: $_ENV['AWS_ACCESS_KEY_ID'],
    secret: $_ENV['AWS_SECRET_ACCESS_KEY'],
    region: $_ENV['AWS_REGION'],
    sessionToken: $_ENV['AWS_SESSION_TOKEN'] ?? null
);
```

### Configuração automática pelo ambiente

`DynamoManager::create()` sem argumentos usa `DynamoConfig::fromEnvironment()`:

| Variável | Uso |
| --- | --- |
| `DYNAMODB_ENDPOINT` (ou `AWS_ENDPOINT_URL_DYNAMODB`) | Endpoint local. Sem `AWS_ACCESS_KEY_ID`, usa credenciais fictícias. |
| `AWS_REGION` (ou `AWS_DEFAULT_REGION`) | Região. Padrão: `us-east-1`. |
| Credenciais | Cadeia padrão do SDK. |

```php
$manager = DynamoManager::create(); // local se DYNAMODB_ENDPOINT existir, AWS caso contrário
```

### Opções do cliente

`clientOptions` é repassado ao `DynamoDbClient` e tem precedência sobre o resto:

```php
$config = new DynamoConfig(region: 'sa-east-1', clientOptions: [
    'retries' => 5,
    'http' => ['timeout' => 5],
]);
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

Os itens são gravados com os atributos `PK` e `SK`. Se a tabela usa outros nomes de chave (por exemplo, `id` e `createdAt`), a biblioteca descobre o schema com `DescribeTable` e renomeia as chaves em todas as operações por chave: put, get, update, delete, lote e transação. Nas expressões de query, use os nomes reais da tabela.

### 2. Operações Básicas

```php
// Criar e salvar
$user = new User();
$user->setEmail('john@example.com')->setName('John Doe');
$manager->save($user);

// Buscar
$foundUser = $manager->find('users', $user->getKey(), User::class);

// Atualizar o item inteiro
$user->setAge(30);
$manager->save($user);

// Atualizar atributos (sem cláusula, assume SET; aceita SET, REMOVE, ADD e DELETE)
$manager->update('users', $user->getKey(), '#n = :n', [':n' => 'John'], null, ['#n' => 'name']);
$manager->update('users', $user->getKey(), 'REMOVE age', []);

// Remover
$manager->delete($user);
```

### 3. Query Builder

```php
$result = $manager->getQueryBuilder(User::class)
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

$repository = new UserRepository($manager);
$user = $repository->findByEmail('john@example.com');
```

### 5. Transações

```php
$transaction = $manager->getTransaction();

$transaction->save($user1);
$transaction->save($user2);
$transaction->deleteItem($oldUser);

$result = $transaction->flush(); // até 100 operações
```

### 6. Operações em Lote

```php
// Inserir múltiplos itens (todos da mesma tabela)
$manager->insertItems($users);

// Remover múltiplos itens
$manager->deleteItems($users);
```

Os itens são enviados antes do retorno, em blocos de 25, e os itens não processados são reenviados. Se o DynamoDB rejeitar uma requisição, é lançada uma `RuntimeException`, com a exceção original da AWS em `getPrevious()`. Para tratar os erros você mesmo, passe um callback:

```php
$manager->insertItems($users, ['error' => fn($e) => $logger->error($e->getMessage())]);
```

## 🐳 Ambiente Local

O `docker-compose.yml` sobe o [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) (porta 8000) e o DynamoDB Admin (`http://localhost:8001`):

```bash
docker compose up -d     # ou: make docker-up
```

> As versões atuais do LocalStack exigem `LOCALSTACK_AUTH_TOKEN`. Por isso o ambiente padrão usa o DynamoDB Local. Para usar LocalStack, aponte `DYNAMODB_ENDPOINT` para `http://localhost:4566`.

## 🧪 Testes

```bash
# Todos os testes (os de integração são pulados sem DYNAMODB_ENDPOINT)
composer test

# Apenas unitários (sem rede; o cliente AWS é simulado)
composer test:unit

# Integração contra o DynamoDB Local
docker compose up -d
DYNAMODB_ENDPOINT=http://localhost:8000 composer test:integration   # ou: make test-integration

# Relatório de cobertura (requer Xdebug ou PCOV)
composer test:coverage
```

## 🔍 Análise de Código

```bash
composer phpstan   # PHPStan nível 8
composer cs        # PSR-12
composer cs:fix    # corrige o que for automático
composer check     # cs + phpstan + testes
```

## 📚 Documentação

```bash
composer docs
```

Requer o [phpDocumentor](https://docs.phpdoc.org/) instalado globalmente (phar ou imagem Docker `phpdoc/phpdoc`). Ele não está no `require-dev` porque depende de pacotes com alertas de segurança que impedem o `composer install`.

## 🛠️ Desenvolvimento

### Estrutura do Projeto

```text
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
- `composer example` - Executar exemplo de uso (requer `DYNAMODB_ENDPOINT`)

### Comandos Make

- `make setup` - Instalar dependências e subir o ambiente local
- `make test` - Executar testes
- `make test-integration` - Testes de integração contra o DynamoDB Local
- `make test-coverage` - Testes com cobertura
- `make phpstan` - Análise estática
- `make cs` / `make cs-fix` - Padrões de código
- `make example` - Executar o exemplo contra o DynamoDB Local
- `make version` - Mostrar versão atual
- `make release-patch` / `release-minor` / `release-major` - Criar release

## 🚀 Release

```bash
make release-patch          # 0.0.x - correções
make release-minor          # 0.x.0 - funcionalidades
make release-major          # x.0.0 - mudanças incompatíveis

./scripts/release.sh 4.0.0  # versão explícita (ex.: a que já está no composer.json)
```

O script:

1. Valida o ambiente (repositório Git, sem mudanças pendentes, branch main/master)
2. Roda `composer check` (phpcs, PHPStan e PHPUnit) e para se algo falhar
3. Atualiza a versão no `composer.json` e cria a entrada no `CHANGELOG.md`. Se a entrada já existir como "Não lançada", ela recebe a data do release.
4. Faz o commit, cria a tag anotada e faz push da branch e da tag

## 🔄 Migração para a v4

1. **Credenciais**: `new DynamoConfig()` sem argumentos não usa mais `test/test`; usa a cadeia padrão da AWS. Para ambiente local, use `DynamoConfig::local()`.
2. **Exceções**: erros do DynamoDB chegam como `DynamoDbException`, não mais como `Exception` genérica. Operações em lote lançam `RuntimeException` quando falham.
3. **Lote**: `insertItems`/`deleteItems` já enviam os itens; não é preciso chamar `flush()` no retorno.
4. **Query Builder**: `$manager->getQueryBuilder(User::class)`.

Veja o [CHANGELOG](CHANGELOG.md) para a lista completa.

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Rode `composer check`
4. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
5. Push para a branch (`git push origin feature/AmazingFeature`)
6. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🆘 Suporte

- **Issues**: [GitHub Issues](https://github.com/jonjon/php-dynamo/issues)
- **Email**: jonathan201392127@gmail.com

## 🙏 Agradecimentos

- AWS SDK for PHP
- DynamoDB Local
- PHPUnit
- PHPStan
- PHP CodeSniffer

---

**Desenvolvido com ❤️ por Jonathan Braga**
