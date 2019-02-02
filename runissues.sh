# Upload the jar
./upload_spark_jar.sh 
export MEMORY_OVERHEAD_FRACTION=0.40

APP_NAME=ISSUES SPARK_DEFAULT_PARALLELISM=60 NUM_EXECS=10 INPUT="gs://tigeys-buckets-are-rad/issue_urls/*.csv" OUTPUT="gs://frank-the-unicorn/issues-full" MAIN_CLASS=com.holdenkarau.predict.pr.comments.sparkProject.dataprep.IssueDataFetchSCApp ./run_spark_data_process.sh
