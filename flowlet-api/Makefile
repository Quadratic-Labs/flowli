.PHONY: help install dev test coverage lint format clean run docs

help:
	@echo "Flowlet Development Commands"
	@echo "=============================="
	@echo "install     - Install package in production mode"
	@echo "dev         - Install package in development mode"
	@echo "test        - Run tests"
	@echo "coverage    - Run tests with coverage report"
	@echo "lint        - Run linting checks"
	@echo "format      - Format code with ruff"
	@echo "typecheck   - Run mypy type checking"
	@echo "clean       - Clean build artifacts"
	@echo "run         - Run development server"
	@echo "build       - Build distribution packages"
	@echo "all         - Run format, lint, typecheck, and test"

install:
	pip install .

dev:
	pip install -e ".[dev]"

test:
	pytest

coverage:
	pytest --cov=flowlet --cov-report=html --cov-report=term-missing

lint:
	ruff check .

format:
	ruff format .

typecheck:
	mypy src/flowlet

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.db" -delete
	find . -type f -name "*.db-journal" -delete

run:
	uvicorn flowlet.app:app --reload --host 0.0.0.0 --port 8000

build:
	python -m build

all: format lint typecheck test
	@echo "✅ All checks passed!"
