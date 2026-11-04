FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install Java 17 (required for PySpark 4.x) and procps (provides `ps` required by Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install dependencies for Spark Streaming Consumer
RUN uv pip install --system --no-cache \
    "pyspark>=4.2.1" \
    "pyarrow>=26.0.0" \
    "pandas>=3.1.0" \
    "google-cloud-storage>=3.1.1" \
    "psycopg2-binary>=2.9.13" \
    "python-dotenv>=1.2.4" \
    "requests>=2.34.2"

# Pre-fetch spark-sql-kafka connector JAR at build time to avoid re-downloading on restart
RUN python -c "from pyspark.sql import SparkSession; from pyspark.version import __version__ as v; \
    scala = '2.13' if int(v.split('.')[0]) >= 4 else '2.12'; \
    SparkSession.builder.master('local[1]').config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_{scala}:{v}').getOrCreate().stop()"

COPY spark/ ./spark/

ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

CMD ["python", "spark/streaming_consumer.py"]

