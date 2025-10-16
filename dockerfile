FROM python:3.11-bullseye AS spark-base

ARG SPARK_VERSION=3.5.7

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*do

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz -o spark-3.5.7-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.7-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.7-bin-hadoop3.tgz

# RUN curl -fL https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark-3.3.1-bin-hadoop3.tgz \
#  && echo "a3d5d7d78736f7ba0b41b7c927c09bf35e1c80e1d7f3c9b2a7d4e8f9a1b2c3d4 spark-3.3.1-bin-hadoop3.tgz" | sha1sum -c - \
#  && tar xvzf spark-3.3.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
#  && rm spark-3.3.1-bin-hadoop3.tgz

COPY ./scripts /app/scripts
COPY ./requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]