#!/bin/bash

SPARK_VERSION=3.2.1
SPARK=spark-$SPARK_VERSION
export SPARK_HOME=/content/$SPARK-bin-hadoop3.2

echo "Downloading Spark..."

wget -q -O /tmp/spark.tgz https://dlcdn.apache.org/spark/$SPARK/$SPARK-bin-hadoop3.2.tgz
echo "Unpacking Spark..."
tar xf /tmp/spark.tgz
pip install -q findspark

REPO=https://github.com/smduarte/ps2022/raw/master/colab/

JARS_LIBS=(spark-streaming_2.12-3.2.1-$SPARK_VERSION.jar )


#JARS_KAFKA=(spark-sql-kafka-0-10_2.11-$SPARK_VERSION.jar \
#               spark-streaming-kafka-0-10-assembly_2.11-$SPARK_VERSION.jar \
#               spark-streaming-kafka-0-10_2.11-$SPARK_VERSION.jar \
#               spark-streaming-kafka-0-8-assembly_2.11-$SPARK_VERSION.jar \
#               spark-streaming-kafka-0-8_2.11-$SPARK_VERSION.jar)

for jar in "${JARS_LIBS[@]}"
do
  echo "Downloading ${jar}..."
  wget -q -O - $REPO/$jar.gz | gunzip -c > $SPARK_HOME/jars/$jar
done

echo "Done"
