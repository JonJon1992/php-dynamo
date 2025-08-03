#!/bin/bash

# Script de Release para php-dynamo
# 
# Este script automatiza o processo de criação de uma nova versão:
# 1. Valida o ambiente
# 2. Executa testes
# 3. Atualiza a versão
# 4. Cria tag Git
# 5. Faz push para o repositório

set -e  # Para o script se qualquer comando falhar

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para imprimir mensagens coloridas
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Função para obter a versão atual do composer.json
get_current_version() {
    local version=$(php -r "
        \$composer = json_decode(file_get_contents('composer.json'), true);
        echo \$composer['version'];
    ")
    echo "$version"
}

# Função para validar o ambiente
validate_environment() {
    print_message $BLUE "🔍 Validando ambiente..."
    
    # Verificar se estamos em um repositório Git
    if [ ! -d ".git" ]; then
        print_message $RED "❌ Não é um repositório Git válido"
        exit 1
    fi

    # Verificar se há mudanças não commitadas
    if [ -n "$(git status --porcelain)" ]; then
        print_message $RED "❌ Existem mudanças não commitadas. Faça commit antes de criar um release."
        exit 1
    fi

    # Verificar se estamos na branch main
    local branch=$(git branch --show-current)
    if [ "$branch" != "main" ] && [ "$branch" != "master" ]; then
        print_message $RED "❌ Deve estar na branch main/master para criar release. Branch atual: $branch"
        exit 1
    fi

    print_message $GREEN "✅ Ambiente validado"
}

# Função para executar testes
run_tests() {
    print_message $BLUE "🧪 Executando testes..."
    
    local commands=("composer test" "composer phpstan" "composer cs")
    
    for command in "${commands[@]}"; do
        print_message $YELLOW "Executando: $command"
        if ! eval "$command" > /dev/null 2>&1; then
            print_message $RED "❌ Falha no comando: $command"
            exit 1
        fi
    done

    print_message $GREEN "✅ Todos os testes passaram"
}

# Função para atualizar a versão
update_version() {
    local version_type=$1
    local current_version=$(get_current_version)
    
    print_message $BLUE "📝 Atualizando versão..."
    
    # Parse da versão atual
    IFS='.' read -ra VERSION_PARTS <<< "$current_version"
    local major=${VERSION_PARTS[0]}
    local minor=${VERSION_PARTS[1]}
    local patch=${VERSION_PARTS[2]}
    
    # Incrementar versão baseado no tipo
    case $version_type in
        "major")
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        "minor")
            minor=$((minor + 1))
            patch=0
            ;;
        "patch")
            patch=$((patch + 1))
            ;;
        *)
            print_message $RED "❌ Tipo de versão inválido: $version_type"
            exit 1
            ;;
    esac
    
    local new_version="$major.$minor.$patch"
    
    # Atualizar composer.json
    php -r "
        \$composer = json_decode(file_get_contents('composer.json'), true);
        \$composer['version'] = '$new_version';
        file_put_contents('composer.json', json_encode(\$composer, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . \"\n\");
    "
    
    # Exportar nova versão para uso posterior
    export NEW_VERSION=$new_version
    
    print_message $GREEN "✅ Versão atualizada de $current_version para $new_version"
}

# Função para atualizar changelog
update_changelog() {
    print_message $BLUE "📋 Atualizando changelog..."
    
    local date=$(date +%Y-%m-%d)
    local new_entry="## [$NEW_VERSION] - $date

### 🚀 Adicionado
- 

### 🔧 Melhorado
- 

### 🐛 Corrigido
- 

### ⚠️ Quebrado
- 

---

"
    
    # Inserir nova entrada no changelog
    awk -v entry="$new_entry" 'NR==3{print entry} 1' CHANGELOG.md > CHANGELOG.md.tmp && mv CHANGELOG.md.tmp CHANGELOG.md
    
    print_message $GREEN "✅ Changelog atualizado"
}

# Função para fazer commit das mudanças
commit_changes() {
    print_message $BLUE "💾 Fazendo commit das mudanças..."
    
    git add composer.json CHANGELOG.md
    git commit -m "chore: bump version to $NEW_VERSION"
    
    print_message $GREEN "✅ Mudanças commitadas"
}

# Função para criar tag
create_tag() {
    print_message $BLUE "🏷️ Criando tag..."
    
    git tag -a "v$NEW_VERSION" -m "Release version $NEW_VERSION"
    
    print_message $GREEN "✅ Tag v$NEW_VERSION criada"
}

# Função para fazer push das mudanças
push_changes() {
    print_message $BLUE "📤 Fazendo push das mudanças..."
    
    local commands=("git push origin main" "git push origin v$NEW_VERSION")
    
    for command in "${commands[@]}"; do
        print_message $YELLOW "Executando: $command"
        if ! eval "$command" > /dev/null 2>&1; then
            print_message $YELLOW "⚠️ Aviso: $command falhou. Execute manualmente se necessário."
        fi
    done
    
    print_message $GREEN "✅ Push concluído"
}

# Função principal
main() {
    local version_type=${1:-patch}
    
    print_message $BLUE "🚀 Iniciando processo de release..."
    echo
    
    # Validar tipo de versão
    if [[ ! "$version_type" =~ ^(major|minor|patch)$ ]]; then
        print_message $RED "❌ Tipo de versão inválido. Use: major, minor ou patch"
        exit 1
    fi
    
    validate_environment
    run_tests
    update_version "$version_type"
    update_changelog
    commit_changes
    create_tag
    push_changes
    
    echo
    print_message $GREEN "✅ Release $NEW_VERSION criado com sucesso!"
    print_message $GREEN "📦 Tag: v$NEW_VERSION"
    print_message $GREEN "🔗 Para fazer deploy, execute: git push origin v$NEW_VERSION"
}

# Verificar se foi fornecido um argumento
if [ $# -eq 0 ]; then
    echo "Uso: $0 [major|minor|patch]"
    echo "  major  - Incrementa versão major (x.0.0)"
    echo "  minor  - Incrementa versão minor (0.x.0)"
    echo "  patch  - Incrementa versão patch (0.0.x) [padrão]"
    exit 1
fi

# Executar função principal
main "$1" 