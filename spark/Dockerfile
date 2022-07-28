FROM bitnami/spark:3.3.0

USER root
ENV SBT_VERSION=1.2.8
ENV SBT_HOME=/usr/local/sbt

RUN mkdir -p /opt/spark-apps && \
    mkdir -p /opt/spark-data

WORKDIR /opt/spark-apps

COPY ./build.sbt .

#Sbt Installation
RUN export PATH="${SBT_HOME}/bin:$PATH" &&  \
    mkdir -p "${SBT_HOME}" &&  \
    curl -sSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | tar xz -C ${SBT_HOME} --strip-components=1 &&  \
    ln -s "${SBT_HOME}/bin/"* "/usr/bin/" && \
    sbt update

RUN mkdir -p "${SPARK_HOME}/.ivy2" && \
    mv /root/.ivy2/cache ${SPARK_HOME}/.ivy2/ && \
    chown -R 1001:1001 ${SPARK_HOME}/.ivy2

COPY ./apps/* /opt/spark-apps/
COPY ./spark/spark-defaults.conf ${SPARK_HOME}/conf/
COPY ./spark/log4j.properties ${SPARK_HOME}/conf/

RUN chmod g+rwX ${SBT_HOME} && \
    chown -R 1001:1001 ${SPARK_HOME}/conf && \
    chown -R 1001:1001 ${SBT_HOME} && \
    chown -R 1001:1001 /opt/spark-apps && \
    chown -R 1001:1001 /opt/spark-data

USER 1001


ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077

