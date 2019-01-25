#!/bin/bash
set -e
set -x
# Auth
gcloud container clusters get-credentials tigeycluster --zone us-central1-a --project boos-demo-projects-are-rad
# Upload the jar
./upload_spark_jar.sh 
# Train the model
export APP_PREFIX="ml3test"
SPARK_EXEC_MEMORY=18g VERSION=dev APP_NAME="$APP_PREFIX$VERSION" NUM_EXECS=30 SPARK_DEFAULT_PARALLELISM=100 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
SPARK_EXEC_MEMORY=18g VERSION=2019 APP_NAME="$APP_PREFIX$VERSION" NUM_EXECS=40 SPARK_DEFAULT_PARALLELISM=200 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
SPARK_EXEC_MEMORY=18g VERSION=2018 APP_NAME="$APP_PREFIX$VERSION" NUM_EXECS=80 SPARK_DEFAULT_PARALLELISM=500 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
SPARK_EXEC_MEMORY=18g VERSION=full APP_NAME="$APP_PREFIX$VERSION" NUM_EXECS=100 SPARK_DEFAULT_PARALLELISM=1000 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
