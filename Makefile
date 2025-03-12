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
