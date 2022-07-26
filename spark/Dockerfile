FROM bitnami/spark:3.3.0

USER root
RUN apt-get update && apt-get install -y maven
#RUN chmod g+rwX /opt/bitnami
#RUN chown -R 1001:1001 /opt/bitnami/spark

RUN cd "/tmp" && \
    curl --location http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz | \
    tar --extract --gzip --strip=1 --directory /opt/bitnami/spark/ spark-3.0.0-bin-hadoop2.7/jars/

RUN mkdir -p /opt/spark-apps && \
    mkdir -p /opt/spark-data
WORKDIR /opt/spark-apps

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077

USER 1001
