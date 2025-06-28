.PHONY: help install test clean run run-spark compare format lint

# Variables
PYTHON = python
PIP = pip
PYTEST = pytest
BLACK = black
FLAKE8 = flake8

# Directorios
SRC_DIR = src
TESTS_DIR = tests
SCRIPTS_DIR = scripts

help: ## Mostrar esta ayuda
	@echo "Comandos disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Instalar dependencias
	$(PIP) install -r requirements.txt

install-dev: ## Instalar dependencias de desarrollo
	$(PIP) install -r requirements.txt
	$(PIP) install pytest-cov black flake8

test: ## Ejecutar tests
	$(PYTEST) $(TESTS_DIR)/ -v

test-cov: ## Ejecutar tests con cobertura
	$(PYTEST) $(TESTS_DIR)/ --cov=$(SRC_DIR) --cov-report=html --cov-report=term

run: ## Ejecutar pipeline principal (Pandas)
	$(PYTHON) main.py

run-spark: ## Ejecutar pipeline Spark
	$(PYTHON) $(SCRIPTS_DIR)/run_spark_pipeline.py

compare: ## Comparar resultados de ambos pipelines
	$(PYTHON) $(SCRIPTS_DIR)/compare_pipelines.py

format: ## Formatear código con Black
	$(BLACK) $(SRC_DIR)/ $(SCRIPTS_DIR)/ $(TESTS_DIR)/

lint: ## Verificar estilo de código con Flake8
	$(FLAKE8) $(SRC_DIR)/ $(SCRIPTS_DIR)/ $(TESTS_DIR)/

clean: ## Limpiar archivos generados
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

setup: ## Configurar el proyecto completo
	mkdir -p output logs models
	$(PIP) install -r requirements.txt

all: setup format lint test run ## Ejecutar todo el pipeline: setup, format, lint, test, run

check: format lint test ## Verificar código: format, lint, test 