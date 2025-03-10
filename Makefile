ipython:
	uv run ipython

beatporter:
	uv run ipython src/beatporter.py

build:
	docker buildx build --platform linux/amd64 -t beatporter:latest .

start:
	./bin/dev/docker-start.sh

ruff:
	uv run ruff check --fix 
