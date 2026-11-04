FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install lightweight dependencies for Producer & Postgres seed/incremental scripts
RUN uv pip install --system --no-cache \
    "confluent-kafka>=2.16.1" \
    "faker>=40.44.0" \
    "psycopg2-binary>=2.9.13" \
    "python-dotenv>=1.2.4"

COPY scripts/ ./scripts/

ENV PYTHONPATH="/app:/app/scripts"
ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/transaction_producer.py"]

