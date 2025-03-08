ipython:
	cd ./src && poetry run ipython

beatporter:
	cd ./src && poetry run python beatporter.py

build:
	docker buildx build --platform linux/amd64 -t beatporter:latest .

start:
	./bin/dev/docker-start.sh

ruff:
	poetry run ruff check --fix 
