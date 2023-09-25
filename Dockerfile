# Oh dear, poetry has not figured out a unified solution to Dockerfiles:
# https://github.com/orgs/python-poetry/discussions/1879

FROM python:3.11-slim AS base
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get -y --no-install-recommends install \
        'build-essential' \
        'libpq-dev' \
    && \
    rm -rf /var/lib/apt/lists/*
ENV POETRY_NO_INTERACTION=true \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_CACHE_DIR=/tmp/poetry_cache
WORKDIR /app
COPY tool-dependencies ./tool-dependencies
RUN pip install --no-cache-dir \
    --requirement /app/tool-dependencies/requirements.txt
COPY pyproject.toml poetry.lock ./



FROM base AS builder
RUN --mount=type=cache,target="${POETRY_CACHE_DIR}" \
    poetry install --without dev --no-root



FROM builder AS tester
COPY . .
RUN --mount=type=cache,target="${POETRY_CACHE_DIR}" \
    poetry install
CMD ["poe", "check"]



FROM python:3.11-slim AS production
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get -y --no-install-recommends install \
        'postgresql' \
        'sudo' \
    && \
    rm -rf /var/lib/apt/lists/*
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY --from=builder "${VIRTUAL_ENV}" "${VIRTUAL_ENV}"
COPY . .
RUN pip install --no-cache-dir \
    --requirement /app/tool-dependencies/requirements.txt && \
    poetry install --without dev && \
    pip uninstall --yes --requirement /app/tool-dependencies/requirements.txt
CMD ["python", "-m", "waltti_apc_vehicle_anonymization_profiler.main"]
