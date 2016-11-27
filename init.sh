#/bin/env bash
source /opt/spark_test/spark_conf/spark-env.sh
export JAVA_HOME=/opt/spark_test/java-dist/jdk1.8.0_111
export PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:$PATH
