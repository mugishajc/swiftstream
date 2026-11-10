.PHONY: install test lint typecheck check run clean docker-up docker-down

install:
	pip install -e ".[dev]"

test:
	python -m pytest tests/ -v --tb=short

test-cov:
	python -m pytest tests/ -v --cov=swiftstream --cov-report=term-missing

lint:
	python -m ruff check swiftstream/ tests/
	python -m ruff format --check swiftstream/ tests/

lint-fix:
	python -m ruff check --fix swiftstream/ tests/
	python -m ruff format swiftstream/ tests/

typecheck:
	python -m mypy swiftstream/

check: lint typecheck test

run:
	python -m swiftstream -c examples/clickstream.yaml -v

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	rm -rf dist/ build/ *.egg-info
