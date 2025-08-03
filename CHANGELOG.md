# Changelog

## [3.0.2] - 2025-08-03

### 🚀 Adicionado
- Suporte ao PHP 8.2+ (atualizado de PHP 8.1+)
- Melhorias de tipo usando recursos do PHP 8.2
- Union types melhorados em métodos de retorno

### 🔧 Melhorado
- Type hints mais precisos usando recursos do PHP 8.2
- Melhor compatibilidade com versões mais recentes do PHP

### 🐛 Corrigido
- 

### ⚠️ Quebrado
- Requer PHP 8.2+ (anteriormente PHP 8.1+)

---


## [3.0.1] - 2025-08-03

### 🚀 Adicionado
- 

### 🔧 Melhorado
- 

### 🐛 Corrigido
- 

### ⚠️ Quebrado
- 

---


Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [3.0.0] - 2024-01-XX

### 🚀 Adicionado
- Suporte ao PHP 8.2+ (atualizado de PHP 8.1)
- AWS SDK atualizado para v3.350 (atualizado de v3.324)
- Strict types em todos os arquivos
- Type hints melhorados em todos os métodos
- Match expressions substituindo switch statements
- Novos métodos utilitários no DynamoManager
- Métodos `toArray()`, `toJson()`, `fromArray()`, `fromJson()` no DynamoItem
- Métodos `tableExists()` e `waitForTableActive()` no Dynamo
- Métodos `getOperationCount()`, `clear()`, `isEmpty()`, `getOperations()` no DynamoTransaction
- Métodos `findByKey()`, `findAll()`, `findFirst()`, `count()`, `exists()` no DynamoRepository
- Métodos `getItemsResult()`, `getFirstResult()`, `reset()` no DynamoQueryBuilder
- Métodos estáticos `create()`, `required()`, `optional()`, `withHydrate()`, `withSerialize()` no DynamoProperty
- Utilitários adicionais no DynamoUtil trait
- Configuração completa de testes com PHPUnit
- Configuração de análise estática com PHPStan
- Configuração de padrões de código com PHP CodeSniffer
- Documentação completa com exemplos
- Scripts de desenvolvimento no composer.json

### 🔧 Melhorado
- Tratamento de erros mais específico com exceções personalizadas
- Validação de tipos mais rigorosa
- Melhor organização do código seguindo PSR-12
- Documentação de métodos mais completa
- Exemplos de uso mais abrangentes
- Configuração de ambiente mais flexível

### 🐛 Corrigido
- Bugs na serialização de itens
- Problemas na conversão de tipos
- Erros na validação de propriedades obrigatórias
- Problemas na hidratação de objetos
- Bugs no mapeamento de chaves de tabela
- Problemas na execução de transações
- Erros na conversão de dados para diferentes formatos

### ⚠️ Quebrado
- Requer PHP 8.2+ (anteriormente PHP 8.1+)
- AWS SDK 3.350+ (anteriormente 3.324+)
- Mudanças na sintaxe de atributos PHP 8
- Alterações na estrutura de configuração

### 🔄 Migração da v2.x

#### 1. Atualizar versão do PHP
```bash
# Verificar versão atual
php -v

# Atualizar para PHP 8.2+ se necessário
```

#### 2. Atualizar dependências
```bash
composer update aws/aws-sdk-php
```

#### 3. Atualizar código

**Antes (v2.x):**
```php
$manager = DynamoManager::createWithCredentials($key, $secret, $region);

switch ($hydrate) {
    case self::HYDRATE_OBJECT:
        // ...
}
```

**Depois (v3.0):**
```php
$config = new DynamoConfig($key, $secret, $region);
$manager = DynamoManager::create($config);

return match ($hydrate) {
    self::HYDRATE_OBJECT => $this->convertToObject($item, $class),
    // ...
};
```

#### 4. Atualizar atributos PHP 8

**Antes (v2.x):**
```php
#[DynamoProperty::required('email', 'string')]
private string $email;
```

**Depois (v3.0):**
```php
#[DynamoProperty(['name' => 'email', 'type' => 'string', 'required' => true])]
private string $email;
```

### 📦 Dependências Atualizadas
- `aws/aws-sdk-php`: `^3.324` → `^3.350`
- `phpunit/phpunit`: `^10.0` → `^10.5`
- Adicionado: `phpstan/phpstan`: `^1.10`
- Adicionado: `squizlabs/php_codesniffer`: `^3.7`
- Adicionado: `phpdocumentor/phpdocumentor`: `^3.4`
- Adicionado: `roave/security-advisories`: `dev-latest`
- Adicionado: `localstack/localstack`: `^3.0`

### 🧪 Testes
- Cobertura de testes unitários
- Testes de integração
- Configuração de cobertura de código
- Relatórios de qualidade

### 📚 Documentação
- README completo e atualizado
- Exemplos de uso práticos
- Guia de migração
- Documentação de API
- Changelog detalhado

---

## [2.0.2] - 2023-XX-XX

### Adicionado
- Suporte inicial ao PHP 8.0
- AWS SDK v3.324
- Funcionalidades básicas do DynamoDB
- Query Builder
- Repository Pattern
- Transações básicas

---

**Nota**: Este changelog documenta as principais mudanças entre versões. Para detalhes específicos, consulte os commits do Git. 
