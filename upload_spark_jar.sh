#!/bin/bash
set -e
set -x
export COMMIT=$(git rev-parse HEAD)
pushd sparkproject/
sbt assembly
gsutil cp ./target/scala-2.11/sparkProject-assembly-0.0.1.jar gs://frank-the-unicorn/jars/$COMMIT.jar
popd
echo $COMMIT
