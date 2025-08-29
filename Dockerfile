FROM python:3.10-slim-bullseye

ENV AIRFLOW_HOME=/airflow
WORKDIR /airflow

COPY uv.lock pyproject.toml ./

RUN pip install --upgrade pip && \
    pip install uv && \
    AIRFLOW_VERSION=3.0.3 && \
    PYTHON_VERSION=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') && \
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    uv pip install --system "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

COPY . .

RUN uv pip install --system .

EXPOSE 8080

CMD ["airflow", "standalone"]