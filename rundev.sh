#!/bin/bash
set -e
set -x
# Auth
gcloud container clusters get-credentials tigeycluster --zone us-central1-a --project boos-demo-projects-are-rad
# Upload the jar
./upload_spark_jar.sh 
# Train the model
export APP_PREFX="ml3test-"
SPARK_EXEC_MEMORY=18g VERSION=dev APP_NAME=$APP_PREFIX-$VERSION NUM_EXECS=20 SPARK_DEFAULT_PARALLELISM=30 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
SPARK_EXEC_MEMORY=18g VERSION=2019 APP_NAME=$APP_PREFIX-$VERSION NUM_EXECS=20 SPARK_DEFAULT_PARALLELISM=30 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
SPARK_EXEC_MEMORY=18g VERSION=2018 APP_NAME=$APP_PREFIX-$VERSION NUM_EXECS=20 SPARK_DEFAULT_PARALLELISM=30 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh
