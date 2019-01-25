#!/bin/bash
set -e
set -x
export COMMIT=$(git rev-parse HEAD)
export TARGET=gs://frank-the-unicorn/jars/$COMMIT.jar
eval "gsutil stat $TARGET"
if [ $? != 0 ]; then
  pushd sparkproject/
  sbt assembly
  gsutil cp ./target/scala-2.11/sparkProject-assembly-0.0.1.jar $TARGET
  popd
fi
echo $COMMIT
