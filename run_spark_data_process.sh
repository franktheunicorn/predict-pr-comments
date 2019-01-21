#!/bin/bash
set -e
set -x
export COMMIT=$(git rev-parse HEAD)
export DOCKER_REPO=${DOCKER_REPO:="gcr.io/boos-demo-projects-are-rad/spark-prod"}
export SPARK_VERSION=${SPARK_VERSION:="v2.4.0-with-deps"}
export INPUT=${INPUT:="gs://tigeys-buckets-are-rad/20190118.csv"}
export OUTPUT=${OUTPUT:="gs://frank-the-unicorn/dev_output"}
export CACHE=${CACHE:="gs://frank-the-unicorn/cache"}
export JAR=${JAR:="gs://frank-the-unicorn/jars/$COMMIT.jar"}
export NUM_EXECS=${NUM_EXECS:="3"}
export SPARK_EXEC_MEMORY=${SPARK_EXEC_MEMORY:="8g"}
pushd $SPARK_HOME
./bin/spark-submit --master k8s://http://127.0.0.1:8001  \
  --deploy-mode cluster --conf \
 spark.kubernetes.container.image=$DOCKER_REPO/spark:$SPARK_VERSION \
 --conf spark.executor.instances=$NUM_EXECS \
 --conf spark.executor.memory=$SPARK_EXEC_MEMORY
 --class com.holdenkarau.predict.pr.comments.sparkProject.DataFetchSCApp \
 --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark3 \
 --conf spark.kubernetes.namespace=spark \
 $JAR \
 $INPUT $OUTPUT $CACHE
popd
