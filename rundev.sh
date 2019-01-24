#!/bin/bash
set -e
set -x
# Auth
gcloud container clusters get-credentials tigeycluster --zone us-central1-a --project boos-demo-projects-are-rad
# Upload the jar
./upload_spark_jar.sh 
# Train the model
SPARK_EXEC_MEMORY=18g VERSION=dev APP_NAME=ml1tester NUM_EXECS=20 SPARK_DEFAULT_PARALLELISM=30 MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.ml.MlSCApp INPUT=gs://frank-the-unicorn/$VERSION/output OUTPUT=gs://frank-the-unicorn/$VERSION/ml ./run_spark_data_process.sh 
