base_path=/opt/spark_test

export SPARK_LOCAL_HOSTNAME=$(hostname)
export JAVA_HOME=${base_path}/java-dist/jdk1.8.0_111
export HADOOP_HOME=${base_path}/hadoop-dist
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
