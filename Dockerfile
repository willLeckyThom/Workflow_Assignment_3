FROM apache/airflow:3.0.6

USER root
RUN mkdir -p /data && chown airflow /data
USER airflow

RUN python -m pip install --no-cache-dir "pymupdf4llm<2" openai requests