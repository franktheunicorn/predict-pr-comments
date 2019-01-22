set -e
set -x
#APP_NAME=dev SPARK_DEFAULT_PARALLELISM=100 NUM_EXECS=10 ./run_spark_data_process.sh
#APP_NAME=year2019 SPARK_DEFAULT_PARALLELISM=200 NUM_EXECS=15 INPUT="gs://tigeys-buckets-are-rad/2019*.csv" OUTPUT="gs://frank-the-unicorn/2019/output" CACHE=gs://frank-the-unicorn/2019/cache ./run_spark_data_process.sh
#APP_NAME=year2018 SPARK_DEFAULT_PARALLELISM=1000 NUM_EXECS=20 INPUT="gs://tigeys-buckets-are-rad/2018*.csv" OUTPUT="gs://frank-the-unicorn/2018/output" CACHE=gs://frank-the-unicorn/2018/cache ./run_spark_data_process.sh
APP_NAME=full  SPARK_DEFAULT_PARALLELISM=3000 NUM_EXECS=30 INPUT="gs://tigeys-buckets-are-rad/*.csv" OUTPUT="gs://frank-the-unicorn/full/output" CACHE=gs://frank-the-unicorn/full/cache ./run_spark_data_process.sh
