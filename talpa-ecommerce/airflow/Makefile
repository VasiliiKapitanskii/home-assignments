.PHONY: build format check test clean req-prepare version1 version2
.DEFAULT_GOAL := build

AIRFLOW_DEPLOY_PATH := _deploy/airflow
REQUIREMENTS_FILE := $(AIRFLOW_DEPLOY_PATH)/dev-requirements.txt
REQUIREMENTS_V1_FILE := $(AIRFLOW_DEPLOY_PATH)/dev-requirements-v1.txt
REQUIREMENTS_V2_FILE := $(AIRFLOW_DEPLOY_PATH)/dev-requirements-v2.txt

build: format check clean

format:
	isort dags/
	black -l 100 dags/

check:
	flake8 dags

test:
	python -m pytest

clean:
	rm -rf .pytest_cache
	rm -rf __pycache__
	find . -name \*.pyc -delete

version1:
	rm -f $(REQUIREMENTS_FILE)
	touch $(REQUIREMENTS_FILE)
	cat $(REQUIREMENTS_V2_FILE) >> $(REQUIREMENTS_FILE)

version2:
	rm -f $(REQUIREMENTS_FILE)
	touch $(REQUIREMENTS_FILE)
	cat $(REQUIREMENTS_V2_FILE) >> $(REQUIREMENTS_FILE)

req-prepare: # optional, only for guys who use poetry
	poetry export -f $(REQUIREMENTS_FILE) --output $(REQUIREMENTS_V2_FILE) --without-hashes
