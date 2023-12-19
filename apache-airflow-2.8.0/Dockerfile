FROM apache/airflow:2.8.0

USER root

RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk8-installer

RUN export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"

USER airflow
RUN pip install apache-airflow-providers-apache-spark
