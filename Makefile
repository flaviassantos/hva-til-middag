# provide ENV=dev to use .env.dev instead of .env
ENV_LOADED :=
ifeq ($(ENV), dev)
    ifneq (,$(wildcard ./.env.dev))
        include .env.dev
        export
				ENV_LOADED := Loaded config from .env.dev
    endif
else
    ifneq (,$(wildcard ./.env))
        include .env
        export
				ENV_LOADED := Loaded config from .env
    endif
endif

.PHONY: help
.DEFAULT_GOAL := help

help: logo ## get a list of all the targets, and their short descriptions
	@# source for the incantation: https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'

compose: ## run docker-compose to start Postgres db and PgAdmin
	@echo "###"
	@echo "#  Assumes you've set up the .env variables"
	@echo "###"
	docker-compose docker-compose.yaml up -d

build: ## build docker image
	@echo "###"
	@echo "#  Assumes you've set up the .env variables"
	@echo "###"
	docker build -t ${DOCKER_USERNAME}/${APPLICATION_NAME}:${VERSION_TAG} .

push: ## push docker image to a remote repository
	@echo "###"
	@echo "#  Assumes you've built the image, see make build"
	@echo "###"
	docker push ${DOCKER_USERNAME}/${APPLICATION_NAME}:${VERSION_TAG}

release:
	docker pull ${DOCKER_USERNAME}/${APPLICATION_NAME}:${VERSION_TAG}
	docker tag  ${DOCKER_USERNAME}/${APPLICATION_NAME}:${VERSION_TAG} ${DOCKER_USERNAME}/${APPLICATION_NAME}:latest
	docker push ${DOCKER_USERNAME}/${APPLICATION_NAME}:latest

environment: ## installs required environment for deployment and corpus generation
	@if [ -z "$(ENV_LOADED)" ]; then \
        echo "Error: Configuration file not found"; \
    else \
				echo "###"; \
				echo "# 🥞: $(ENV_LOADED)"; \
				echo "###"; \
	fi
	python -m pip install -qqq -r requirements.txt

requirements: ## create a requirements.txt with only packages used in scripts in this project
	pip3 install pipreqs
	pipreqs ./flows --savepath ../requirements.txt

logo:  ## prints the logo
	@cat logo.txt; echo "\n"