FROM python:3.12-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

FROM base AS runtime
COPY swiftstream/ ./swiftstream/
COPY examples/ ./examples/

EXPOSE 8000 8080

ENTRYPOINT ["python", "-m", "swiftstream"]
CMD ["-c", "examples/clickstream.yaml"]
