FROM python:3.10.4

# Set environment variables to prevent Python from writing .pyc files and to buffer stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set environment variable for Poetry to not create a virtual environment inside the Docker container
ENV POETRY_VIRTUALENVS_CREATE=false

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential

# # Install gcloud SDK
# RUN curl -sSL https://sdk.cloud.google.com | bash

# Install poetry and set path to it
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:${PATH}"

# COPY src /src
# COPY logs /logs
# COPY data /data
# COPY pyproject.toml poetry.lock /src/
COPY . .

WORKDIR /src

# Set the PYTHONPATH to include /src
ENV PYTHONPATH=$PYTHONPATH:/src

EXPOSE 80
EXPOSE 8080
EXPOSE 65000

# Configure poetry:
# - Disable creation of virtual environment by poetry itself as we use the container environment
# - Install all dependencies globally
RUN poetry config virtualenvs.path --unset
RUN poetry config virtualenvs.create false

# Install dependencies using poetry
RUN poetry install --no-root

CMD exec poetry run python beatporter.py
