# Makefile template derivated from https://github.com/dunglas/symfony-docker/blob/main/docs/makefile.md
.DEFAULT_GOAL = help
.PHONY        = help build build-prod up down logs test

KIND = service
NAME = dispatcher
REPOSITORY = ceherzog/$(KIND)-$(NAME)

## —— 🦉 CodeClarity's plugin Makefile 🦉 ——————————————————————————————————
help: ## Outputs this help screen
	@grep -E '(^[a-zA-Z0-9_-]+:.*?##.*$$)|(^##)' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}{printf "\033[32m%-30s\033[0m %s\n", $$1, $$2}' | sed -e 's/\[32m##/[33m/'

## —— Docker 🐳 ————————————————————————————————————————————————————————————————
build-prod: ## Builds the prod Docker images
	@docker build \
	-f .cloud/docker/Dockerfile \
	--target plugin \
	--build-arg PLUGINNAME=$(NAME) \
	--tag $(REPOSITORY):latest \
	.
	
build: ## Builds the dev Docker images
	@cd ../../../.cloud/scripts && sh build.sh $(KIND)-$(NAME)

build-debug: ## Builds the debug Docker images
	@cd ../../../.cloud/scripts && sh build-debug.sh $(KIND)-$(NAME)

up: ## Starts the Docker images
	@cd ../../../.cloud/scripts && sh up.sh $(KIND)-$(NAME)

up-debug: ## Starts the Docker images
	@cd ../../../.cloud/scripts && sh up-debug.sh $(KIND)-$(NAME)

down: ## Stops the Docker images
	@cd ../../../.cloud/scripts && sh down.sh $(KIND)-$(NAME)

logs: ## Show compose logs
	@cd ../../../.cloud/scripts && sh logs.sh $(KIND)-$(NAME)

test: ## Start test and benchmark
	@echo "------ Run test -----"
	go test ./... -coverprofile=./tests/results/coverage.out
	@echo "\n------ Display coverage -----"
	go tool cover -html=./tests/results/coverage.out
	@echo "\n------ Start benchmark -----"
	go test -bench=Create ./tests -run=^# -benchmem -benchtime=10s -cpuprofile=./tests/results/cpu.out -memprofile=./tests/results/mem.out
	go tool pprof -http=:8080 ./tests/results/cpu.out
	go tool pprof -http=:8080 ./tests/results/mem.out