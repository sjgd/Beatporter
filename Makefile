ipython:
	uv run --frozen ipython

beatporter:
	export PYTHONPATH="$(pwd):$(pwd)/src" && uv run --frozen python src/beatporter.py

build:
	docker buildx build --platform linux/amd64 -t beatporter:latest .

start:
	./bin/dev/docker-start.sh

stop:
	./bin/dev/docker-stop.sh

ruff:
	uv run ruff check --fix

tests:
	export PYTHONPATH="$(pwd):$(pwd)/src" && uv run pytest ./tests/core/ --cov=./src --cov-report=term-missing

# Run code quality commands
lint:
	uv run ruff check --fix ./src || true && \
	uv run ruff format ./src || true && \
	uv run mypy --config-file pyproject.toml ./src ./tests || true && \
	uv run pydocstyle ./src ./tests



