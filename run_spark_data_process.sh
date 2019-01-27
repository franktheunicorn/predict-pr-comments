#!/bin/bash
set -e
set -x
export COMMIT=$(git rev-parse HEAD)
export DOCKER_REPO=${DOCKER_REPO:="gcr.io/boos-demo-projects-are-rad/spark-prod"}
export SPARK_VERSION=${SPARK_VERSION:="v2.4.0-with-deps-and-openblas"}
export INPUT=${INPUT:="gs://tigeys-buckets-are-rad/20190118.csv"}
export OUTPUT=${OUTPUT:="gs://frank-the-unicorn/dev/output"}
export CACHE=${CACHE:="gs://frank-the-unicorn/dev/cache"}
export JAR=${JAR:="gs://frank-the-unicorn/jars/$COMMIT.jar"}
export NUM_EXECS=${NUM_EXECS:="3"}
export SPARK_EXEC_MEMORY=${SPARK_EXEC_MEMORY:="19g"}
export SPARK_DRIVER_MEMORY=${SPARK_EXEC_MEMORY:="40g"}
export MEMORY_OVERHEAD_FRACT=${MEMORY_OVERHEAD_FRACK:="0.3"}
export SPARK_DEFAULT_PARALLELISM=${SPARK_DEFAULT_PARALLELISM:="5000"}
export APP_NAME=${APP_NAME:="spark-data-fetcher"}
export MAIN_CLASS=${MAIN_CLASS:="com.holdenkarau.predict.pr.comments.sparkProject.DataFetchSCApp"}
export SPARK_HOME=${SPARK_HOME:="$HOME/Downloads/spark-2.4.0-bin-hadoop2.7/"}
export RUNDATE=$(date +"%s")
export COMMIT_PREFIX=$(git rev-parse HEAD | cut -c 1-8)
export APP_NAME="$APP_NAME-$RUNDATE-$COMMIT_PREFIX"
pushd $SPARK_HOME
./bin/spark-submit --master k8s://http://127.0.0.1:8001  \
  --deploy-mode cluster --conf \
 spark.kubernetes.container.image=$DOCKER_REPO/spark:$SPARK_VERSION \
 --conf spark.executor.instances=$NUM_EXECS \
 --conf spark.executor.memory=$SPARK_EXEC_MEMORY \
 --conf spark.kryoserializer.buffer.max="6m" \
 --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
 --class $MAIN_CLASS \
 --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark3 \
 --conf spark.kubernetes.namespace=spark \
 --conf spark.kubernetes.memoryOverheadFactor=$MEMORY_OVERHEAD_FRACT \
 --conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
 --conf spark.app.name=$APP_NAME \
 $JAR \
 $INPUT $OUTPUT $CACHE
popd
