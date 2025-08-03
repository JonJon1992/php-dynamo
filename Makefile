.PHONY: help install test test-unit test-integration test-coverage phpstan cs cs-fix docs clean docker-up docker-down example

help: ## Mostra esta ajuda
	@echo "Comandos disponíveis:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Instala as dependências
	composer install

test: ## Executa todos os testes
	composer test

test-unit: ## Executa apenas testes unitários
	composer test:unit

test-integration: ## Executa apenas testes de integração
	composer test:integration

test-coverage: ## Executa testes com cobertura
	composer test:coverage

phpstan: ## Executa análise estática
	composer phpstan

cs: ## Verifica padrões de código
	composer cs

cs-fix: ## Corrige padrões de código automaticamente
	composer cs:fix

docs: ## Gera documentação
	composer docs

clean: ## Limpa arquivos temporários
	rm -rf coverage/
	rm -rf docs/
	rm -rf .phpunit.cache/
	rm -f .phpunit.result.cache
	rm -f coverage.txt

docker-up: ## Inicia os containers Docker
	docker-compose up -d

docker-down: ## Para os containers Docker
	docker-compose down

example: ## Executa o exemplo de uso
	composer example

check: ## Executa todas as verificações
	composer check

setup: ## Configura o ambiente de desenvolvimento
	composer install
	docker-compose up -d
	@echo "Ambiente configurado! Acesse http://localhost:8001 para o DynamoDB Admin"

dev: ## Inicia o ambiente de desenvolvimento
	@echo "Iniciando ambiente de desenvolvimento..."
	@echo "LocalStack: http://localhost:4566"
	@echo "DynamoDB Admin: http://localhost:8001"
	docker-compose up

release: ## Cria uma nova versão (patch)
	./scripts/release.sh patch

release-patch: ## Cria uma nova versão patch (0.0.x)
	./scripts/release.sh patch

release-minor: ## Cria uma nova versão minor (0.x.0)
	./scripts/release.sh minor

release-major: ## Cria uma nova versão major (x.0.0)
	./scripts/release.sh major

version: ## Mostra a versão atual
	@echo "Versão atual: $(shell php -r "echo json_decode(file_get_contents('composer.json'), true)['version'];")" 