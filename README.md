# SwiftStream

A Python event processing pipeline that consumes from Kafka, runs messages through a configurable chain of transforms, and writes to multiple output sinks.

## Architecture

```
Kafka Topics ──> Consumer ──> Transform Chain ──> Sinks
                    |              |                |
                    |         filter, enrich,    PostgreSQL
                    |         deduplicate,       S3 (Parquet)
                    |         aggregate          stdout
                    |
                    └──> Dead-Letter Queue (failed messages)
                    
Prometheus (/metrics) + Health Check (/health, /ready)
```

**Key design decisions:**

- Manual offset commits after successful processing for at-least-once delivery
- Dead-letter queue routing after configurable retry count
- Backpressure control via bounded internal buffer
- Tumbling-window aggregation with in-memory state
- LRU deduplication cache with TTL expiry
- Parquet output to S3/MinIO with Snappy compression
- YAML pipeline definitions with environment variable overrides
- Graceful shutdown on SIGTERM with sink flush and offset commit

## Project Structure

```
swiftstream/
  __main__.py       CLI entry point
  config.py         YAML + env var configuration
  consumer.py       Kafka consumer with offset management
  producer.py       Kafka producer for DLQ and output topics
  pipeline.py       Pipeline orchestrator
  metrics.py        Prometheus counters, histograms, gauges
  health.py         HTTP health/readiness endpoint
  transforms/
    base.py         Transform base class
    filter.py       Field-based message filtering
    enrich.py       Static/computed field enrichment
    aggregate.py    Tumbling-window aggregation
    deduplicate.py  LRU dedup with TTL
  sinks/
    base.py         Sink base class
    postgres.py     PostgreSQL JSONB sink with batch inserts
    s3.py           S3/MinIO Parquet sink
    stdout.py       JSON line output for debugging
```

## Quick Start

### Local development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
make test

# Lint and type check
make check
```

### Docker Compose (full stack)

Starts Kafka (KRaft mode), PostgreSQL, MinIO, Prometheus, and the pipeline:

```bash
docker compose up -d

# View logs
docker compose logs -f swiftstream

# Tear down
docker compose down -v
```

### Run a pipeline

```bash
python -m swiftstream -c examples/clickstream.yaml -v
```

## Configuration

Pipelines are defined in YAML. Environment variables override any YAML value using the `SWIFTSTREAM_<SECTION>_<KEY>` naming convention.

### Example: clickstream pipeline

```yaml
name: clickstream-pipeline
input_topics: [user-clicks, page-views]

kafka:
  bootstrap_servers: localhost:9092
  group_id: clickstream-consumers

transforms:
  - name: drop-bots
    type: filter
    params:
      field: user_agent
      op: not_in
      value: ["Googlebot", "Bingbot"]

  - name: add-metadata
    type: enrich
    params:
      static_fields:
        pipeline_version: "1.0"
      computed_fields:
        processed_at: timestamp_iso

  - name: dedup
    type: deduplicate
    params:
      key_fields: [user_id, page_url, timestamp]
      ttl_seconds: 300

sinks:
  - name: pg
    type: postgres
  - name: archive
    type: s3

dead_letter:
  topic: clickstream-dlq
  max_retries: 3
```

### Environment variable overrides

```bash
export SWIFTSTREAM_KAFKA_BOOTSTRAP_SERVERS=broker:9092
export SWIFTSTREAM_POSTGRES_HOST=db.internal
export SWIFTSTREAM_POSTGRES_PASSWORD=secret
export SWIFTSTREAM_S3_ENDPOINT_URL=https://s3.amazonaws.com
```

## Transforms

| Type | Purpose | Key params |
|------|---------|------------|
| `filter` | Drop messages that fail a condition | `field`, `op`, `value`, `negate` |
| `enrich` | Add fields (static, computed, copied) | `static_fields`, `computed_fields`, `copy_fields` |
| `deduplicate` | Suppress duplicates via LRU cache | `key_fields`, `ttl_seconds`, `max_cache_size` |
| `aggregate` | Tumbling-window aggregation | `group_by`, `window_seconds`, `aggregations` |

Filter operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `contains`, `exists`

Computed enrichment functions: `timestamp_iso`, `processing_timestamp_ms`, `source_topic`, `source_partition`, `source_offset`

## Sinks

| Type | Output format | Batching |
|------|--------------|----------|
| `postgres` | JSONB rows with topic/key/value/headers columns | Configurable batch size, `execute_values` bulk insert |
| `s3` | Parquet files (Snappy compressed) partitioned by date/hour | Configurable batch size per file |
| `stdout` | JSON lines | Immediate |

## Observability

### Prometheus metrics (default port 8000)

- `swiftstream_messages_consumed_total` - Messages consumed per topic
- `swiftstream_messages_processed_total` - Successfully processed messages
- `swiftstream_messages_failed_total` - Failed messages
- `swiftstream_messages_dlq_total` - Messages sent to dead-letter queue
- `swiftstream_processing_latency_seconds` - End-to-end processing time histogram
- `swiftstream_transform_latency_seconds` - Per-transform latency histogram
- `swiftstream_sink_write_latency_seconds` - Per-sink write time histogram
- `swiftstream_buffer_size` - Current backpressure buffer gauge
- `swiftstream_consumer_lag` - Estimated consumer lag

### Health check (default port 8080)

- `GET /health` - Liveness check (always 200)
- `GET /ready` - Readiness check (200 when pipeline is running, 503 otherwise)

## Testing

```bash
# Unit tests
make test

# With coverage
make test-cov

# Lint + type check + tests
make check
```

## License

MIT
