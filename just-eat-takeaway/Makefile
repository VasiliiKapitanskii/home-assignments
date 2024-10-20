#!make
PATH_COMPOSE_FILE=./docker/docker-compose.yaml

OS := $(shell uname)
ifeq ($(OS),Linux)
	PYTHON := python3.10
	ACTIVATE := .venv/bin/activate
else ifeq ($(OS),Darwin) # Mac
	PYTHON := python3.10
	ACTIVATE := .venv/bin/activate
else # Windows
	PYTHON := py -3.10
	ACTIVATE := .venv/Scripts/activate
endif

infra-up:
	docker-compose -f ${PATH_COMPOSE_FILE} up;

infra-down:
	docker-compose -f ${PATH_COMPOSE_FILE} down;

infra-reset:
	docker-compose -f ${PATH_COMPOSE_FILE} down
	docker-compose -f ${PATH_COMPOSE_FILE} up -d

sql-lint-allmodels:
	sqlfluff lint --config ./dbt/.sqlfluff ./dbt/models/

sql-fix-allmodels:
	sqlfluff fix --config ./dbt/.sqlfluff --force ./dbt/models/

setup:
	make venv
	( \
		source $(ACTIVATE); \
		source ./.env; \
		make init; \
	)

venv:
	rm -rf .venv
	$(PYTHON) -m venv .venv
	( \
		source $(ACTIVATE); \
		python -m pip install --upgrade pip; \
		python -m pip install -r ./requirements.txt; \
		pre-commit install; \
	)

init:
	cd dbt; dbt clean; dbt deps

deps:
	cd dbt; rm -rf ./dbt_packages/;	dbt deps

.PHONY: check-clickhouse
check-clickhouse:
	@echo "Checking connection to ClickHouse..."
	@python -c "\
import clickhouse_connect; \
client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password=''); \
result = client.query('SELECT version()').result_rows; \
print(f'ClickHouse Version: {result[0][0]}')" \
	&& echo "Connection successful!" \
	|| echo "Connection failed!"
