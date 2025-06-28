.PHONY: help install test clean run run-spark compare format lint

# Variables
PYTHON = python
PIP = pip
PYTEST = pytest
BLACK = black
FLAKE8 = flake8

# Directories
SRC_DIR = src
TESTS_DIR = tests
SCRIPTS_DIR = scripts

help: ## Show this help
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	$(PIP) install -r requirements.txt

install-dev: ## Install development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install pytest-cov black flake8

test: ## Run tests
	$(PYTEST) $(TESTS_DIR)/ -v

test-cov: ## Run tests with coverage
	$(PYTEST) $(TESTS_DIR)/ --cov=$(SRC_DIR) --cov-report=html --cov-report=term

run: ## Run main pipeline (Pandas)
	$(PYTHON) main.py

run-spark: ## Run Spark pipeline
	$(PYTHON) $(SCRIPTS_DIR)/run_spark_pipeline.py

compare: ## Compare results from both pipelines
	$(PYTHON) $(SCRIPTS_DIR)/compare_pipelines.py

format: ## Format code with Black
	$(BLACK) $(SRC_DIR)/ $(SCRIPTS_DIR)/ $(TESTS_DIR)/

lint: ## Check code style with Flake8
	$(FLAKE8) $(SRC_DIR)/ $(SCRIPTS_DIR)/ $(TESTS_DIR)/

clean: ## Clean generated files
	rm -rf output/*
	rm -rf logs/*
	rm -rf models/*
	rm -rf __pycache__/
	rm -rf $(SRC_DIR)/__pycache__/
	rm -rf $(TESTS_DIR)/__pycache__/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type f -name "*.pyc" -delete

setup: ## Setup complete project
	mkdir -p output logs models
	$(PIP) install -r requirements.txt

all: setup format lint test run ## Run complete pipeline: setup, format, lint, test, run

check: format lint test ## Check code: format, lint, test 