FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY dqwatch ./dqwatch
RUN pip install --no-cache-dir .
COPY dagster.yaml ./
ENV DQ_META=/data/meta.sqlite DQ_DATA=/data/data.duckdb DQ_INBOX=/data/inbox DAGSTER_HOME=/data/dagster
