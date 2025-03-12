FROM python:3.13.2

# Set environment variables to prevent Python from writing .pyc files and to buffer stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the uv installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

COPY src /beatporter/src
COPY logs /beatporter//logs
COPY data /beatporter/data
COPY pyproject.toml uv.lock /beatporter/

WORKDIR /beatporter

# Set the PYTHONPATH to include /src
ENV PYTHONPATH=$PYTHONPATH:/beatporter:/beatporter/src

EXPOSE 80
EXPOSE 8080
EXPOSE 65000

# Install dependencies using poetry
RUN uv sync --frozen

CMD exec uv run --frozen python src/beatporter.py
