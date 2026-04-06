FROM apache/spark:3.5.0

USER root

# install python + pip (บาง image ไม่มีครบ)
RUN apt-get update && apt-get install -y python3-pip

# install python libs
RUN pip3 install --no-cache-dir \
    cassandra-driver \
    kafka-python \
    pandas \
    requests

USER spark

RUN mkdir -p /opt/spark/jars

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar /opt/spark/jars/