# Stage 1: Builder
FROM python:3.13.2-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
ENV UV_COMPILE_BYTECODE=1
RUN mkdir -p /app/tmp
ENV TMPDIR=/app/tmp
RUN uv sync --frozen --no-install-project --no-dev --no-cache
RUN rm -rf /app/tmp

# Stage 2: Runner
FROM python:3.13.2-slim AS runner

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:/app/src"

WORKDIR /app

# Copy the virtual environment from the builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code and directories
COPY src /app/src
COPY logs /app/logs
COPY data /app/data

# Expose ports
EXPOSE 80
EXPOSE 8080
EXPOSE 65000

# Run the application
CMD ["python", "src/beatporter.py"]
