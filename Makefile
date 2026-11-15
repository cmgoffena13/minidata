.PHONY: ready format lint type-check test test-cov install upgrade run clean reset query


install:
	git init
	uv sync --all-extras
	uv run -- prek install
	test -f .env || cp .env.example .env
	test -d datawarehouse || mkdir datawarehouse

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache .coverage

reset: clean
	uv venv --clear
	$(MAKE) install

upgrade:
	uv sync --upgrade --all-extras

ready: lint format type-check test-cov

format: 
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

type-check:
	uv run -- ty check

test:
	uv run -- pytest -v -n auto

test-cov:
	uv run -- pytest -v -n auto --cov=src --cov-report=term-missing

run:
	uv run -- python main.py

ifeq ($(firstword $(MAKECMDGOALS)),query)
  QUERY_SQL := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
endif

query:
	@test -n "$(QUERY_SQL)" || (echo "Usage: make query 'SELECT * FROM bronze.widgets'"; exit 1)
	uv run -- minidata query "$(QUERY_SQL)"

ifeq ($(firstword $(MAKECMDGOALS)),query)
%:
	@:
endif

