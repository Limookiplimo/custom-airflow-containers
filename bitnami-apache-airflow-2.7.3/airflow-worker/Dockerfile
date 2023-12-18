FROM bitnami/airflow-worker:2.7.3

USER root

# Install build dependencies
RUN apt-get update -yqq && \
    apt-get install -y --no-install-recommends build-essential python3-dev default-libmysqlclient-dev pkg-config wget vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /requirements.txt

# Install additional dependencies for the specified constraints
RUN pip install --no-cache-dir -r /requirements.txt

USER user
