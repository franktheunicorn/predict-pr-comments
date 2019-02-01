FROM gcr.io/boos-demo-projects-are-rad/spark-prod/spark:v2.4.0

# Manually update Guava deleting the old JAR to ensure we don’t have class path conflicts
RUN rm $SPARK_HOME/jars/guava-*.jar
ADD http://central.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar $SPARK_HOME/jars
# Add the GCS connectors
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar $SPARK_HOME/jars
# Add the Azure Hadoop/Storage JARs
ADD http://central.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.0/hadoop-azure-2.7.0.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.0/azure-storage-7.0.0.jar $SPARK_HOME/jars
# BLAS
RUN apk --no-cache --update-cache add openblas-dev libstdc++ openblas

ENTRYPOINT [ '/opt/entrypoint.sh' ]