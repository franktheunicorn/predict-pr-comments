package com.holdenkarau.predict.pr.comments.sparkProject.helper
/**
 * Test of the Patch extractor
 */

import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.PatchRecord

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class PatchExtractorTest extends FunSuite {
  val simpleInput = """
From 97d57259eaf8ca29ce56a194de110d526c2d1629 Mon Sep 17 00:00:00 2001
From: songchenwen <me@songchenwen.com>
Date: Wed, 16 Jan 2019 19:12:23 +0800
Subject: [PATCH] Feature: SOURCE-IP-CIDR rule type

---
 README.md                  |  1 +
 adapters/inbound/http.go   |  4 +++-
 adapters/inbound/https.go  |  4 +++-
 adapters/inbound/socket.go |  1 +
 adapters/inbound/util.go   |  7 +++++++
 config/config.go           |  4 +++-
 constant/metadata.go       |  1 +
 constant/rule.go           |  3 +++
 rules/ipcidr.go            | 23 ++++++++++++++---------
 tunnel/tunnel.go           |  6 +++---
 10 files changed, 39 insertions(+), 15 deletions(-)

diff --git a/README.md b/README.md
index 31c2bb2..ec256ba 100644
--- a/README.md
+++ b/README.md
@@ -170,6 +170,7 @@ Rule:
 - DOMAIN,google.com,Proxy
 - DOMAIN-SUFFIX,ad.com,REJECT
 - IP-CIDR,127.0.0.0/8,DIRECT
+- SOURCE-IP-CIDR,192.168.1.201/32,DIRECT
 - GEOIP,CN,DIRECT
 # FINAL would remove after prerelease
 # you also can use `FINAL,Proxy` or `FINAL,,Proxy` now
diff --git a/adapters/inbound/http.go b/adapters/inbound/http.go
index 01aa14b..8aa21e7 100644
--- a/adapters/inbound/http.go
+++ b/adapters/inbound/http.go
@@ -32,8 +32,10 @@ func (h *HTTPAdapter) Conn() net.Conn {
 
 // NewHTTP is HTTPAdapter generator
 func NewHTTP(request *http.Request, conn net.Conn) *HTTPAdapter {
+	metadata := parseHTTPAddr(request)
+	metadata.SourceIP = parseSourceIP(conn)
 	return &HTTPAdapter{
-		metadata: parseHTTPAddr(request),
+		metadata: metadata,
 		R:        request,
 		conn:     conn,
 	}
"""

  test("Simple input") {
    val results = PatchExtractor.processPatch(simpleInput)
    val expected = List(
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        172,173, 4,
        "- SOURCE-IP-CIDR,192.168.1.201/32,DIRECT",
        "README.md",
        true),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        34,35, 4,
	"	metadata := parseHTTPAddr(request)",
        "adapters/inbound/http.go",
        true),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        34,36, 5,
        "	metadata.SourceIP = parseSourceIP(conn)",
        "adapters/inbound/http.go",
        true),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        36,37, 7,
        "		metadata: parseHTTPAddr(request),",
        "adapters/inbound/http.go",
        false),
      PatchRecord("97d57259eaf8ca29ce56a194de110d526c2d1629",
        36,38, 8,
        "		metadata: metadata,",
        "adapters/inbound/http.go",
        true))
    results should contain theSameElementsAs expected
  }
  val simpleDiffInput = """
diff --git a/run_spark_data_process.sh b/run_spark_data_process.sh
index bd71564..e0d635d 100755
--- a/fuck/run_spark_data_process.sh
+++ b/fuck/run_spark_data_process.sh
@@ -36,5 +36,5 @@ pushd $SPARK_HOME
  --conf spark.rpc.askTimeout=300 \
  --conf spark.locality.wait=2 \
  $JAR \
- $INPUT $OUTPUT $CACHE
+ $INPUT $OUTPUT $CACHE $ISSUES
 popd
diff --git a/rundev.sh b/rundev.sh
index d272c48..d408ab2 100755
--- a/rundev.sh
+++ b/rundev.sh
@@ -6,22 +6,23 @@ gcloud container clusters get-credentials tigeycluster --zone us-central1-a --pr
 # Upload the jar
 ./upload_spark_jar.sh 
 # Train the model
-export APP_PREFIX="ml22a-gbt-withcv-test"
+export APP_PREFIX="ml23a-gbt-withcv-withissues-test"
 export MEMORY_OVERHEAD_FRACTION=0.40
 export SPARK_EXEC_MEMORY=35g
"""
  test("Simple diff input") {
    val results = PatchExtractor.processPatch(simpleDiffInput)
    val expected = List(
      PatchRecord(null,
        39,38, 4,
        " $INPUT $OUTPUT $CACHE",
        "fuck/run_spark_data_process.sh",
        false),
      PatchRecord(null,
        39, 39, 5,
	" $INPUT $OUTPUT $CACHE $ISSUES",
        "fuck/run_spark_data_process.sh",
        true),
      PatchRecord(null,
        9,8, 4,
        "export APP_PREFIX=\"ml22a-gbt-withcv-test\"",
        "rundev.sh",
        false),
      PatchRecord(null,
        9,9, 5,
        "export APP_PREFIX=\"ml23a-gbt-withcv-withissues-test\"",
        "rundev.sh",
        true))
    results should contain theSameElementsAs expected
  }

  val slightlyComplexInput = """
From a7fbc74335c2df27002e8158f8e83a919195eed7 Mon Sep 17 00:00:00 2001
From: Holden Karau <holden@pigscanfly.ca>
Date: Mon, 6 Aug 2018 11:04:31 -0700
Subject: [PATCH 1/7] [SPARK-21436] Take advantage of known partioner for
 distinct on RDDs to avoid a shuffle. Special case the situation where we know
 the partioner and the number of requested partions output is the same as the
 current partioner to avoid a shuffle and instead compute distinct inside of
 each partion.

---
 core/src/main/scala/org/apache/spark/rdd/RDD.scala   | 11 ++++++++++-
 .../test/scala/org/apache/spark/rdd/RDDSuite.scala   | 12 ++++++++++++
 2 files changed, 22 insertions(+), 1 deletion(-)

diff --git a/core/src/main/scala/org/apache/spark/rdd/RDD.scala b/core/src/main/scala/org/apache/spark/rdd/RDD.scala
index 0574abdca32ac..471b9e0a1a877 100644
--- a/core/src/main/scala/org/apache/spark/rdd/RDD.scala
+++ b/core/src/main/scala/org/apache/spark/rdd/RDD.scala
@@ -396,7 +396,16 @@ abstract class RDD[T: ClassTag](
    * Return a new RDD containing the distinct elements in this RDD.
    */
   def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
-    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
+    // If the data is already approriately partioned with a known partioner we can work locally.
+    def removeDuplicatesInPartition(itr: Iterator[T]): Iterator[T] = {
+      val set = new mutable.HashSet[T]() ++= itr
+      set.toIterator
+    }
+    partitioner match {
+      case Some(p) if numPartitions == partitions.length =>
+        mapPartitions(removeDuplicatesInPartition, preservesPartitioning = true)
+      case _ => map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
+    }
   }
 
   /**
diff --git a/core/src/test/scala/org/apache/spark/rdd/RDDSuite.scala b/core/src/test/scala/org/apache/spark/rdd/RDDSuite.scala
index b143a468a1baf..3001a2b005d8b 100644
--- a/core/src/test/scala/org/apache/spark/rdd/RDDSuite.scala
+++ b/core/src/test/scala/org/apache/spark/rdd/RDDSuite.scala
@@ -95,6 +95,18 @@ class RDDSuite extends SparkFunSuite with SharedSparkContext {
     assert(!deserial.toString().isEmpty())
   }
 
+  test("distinct with known partioner does not cause shuffle") {
+    val rdd = sc.parallelize(1.to(100), 10).map(x => (x % 10, x % 10)).sortByKey()
+    val initialPartioner = rdd.partitioner
+    val distinctRdd = rdd.distinct()
+    val resultingPartioner = distinctRdd.partitioner
+    assert(initialPartioner === resultingPartioner)
+    val distinctRddDifferent = rdd.distinct(5)
+    val distinctRddDifferentPartioner = distinctRddDifferent.partitioner
+    assert(initialPartioner != distinctRddDifferentPartioner)
+    assert(distinctRdd.collect().sorted === distinctRddDifferent.collect().sorted)
+  }
+
   test("countApproxDistinct") {
 
     def error(est: Long, size: Long): Double = math.abs(est - size) / size.toDouble

From 5fd36592a26b07fdb58e79e4efbb6b70daea54df Mon Sep 17 00:00:00 2001
From: Holden Karau <holden@pigscanfly.ca>
Date: Fri, 10 Aug 2018 11:10:31 -0700
Subject: [PATCH 2/7] CR feedback, reduce # of passes over data from 2 to 1 and
 fix some spelling issues.

---
 core/src/main/scala/org/apache/spark/rdd/RDD.scala   |  6 +++---
 .../test/scala/org/apache/spark/rdd/RDDSuite.scala   | 12 ++++++------
 2 files changed, 9 insertions(+), 9 deletions(-)

diff --git a/core/src/main/scala/org/apache/spark/rdd/RDD.scala b/core/src/main/scala/org/apache/spark/rdd/RDD.scala
index 471b9e0a1a877..d9eff9f9b0ac1 100644
--- a/core/src/main/scala/org/apache/spark/rdd/RDD.scala
+++ b/core/src/main/scala/org/apache/spark/rdd/RDD.scala
@@ -396,10 +396,10 @@ abstract class RDD[T: ClassTag](
    * Return a new RDD containing the distinct elements in this RDD.
    */
   def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
-    // If the data is already approriately partioned with a known partioner we can work locally.
+    // If the data is already approriately partitioned with a known partitioner we can work locally.
     def removeDuplicatesInPartition(itr: Iterator[T]): Iterator[T] = {
-      val set = new mutable.HashSet[T]() ++= itr"""
  test("Slightlycomplexinput") {
    val results = PatchExtractor.processPatch(slightlyComplexInput)
    val commits = results.map(_.commitId).distinct
    val expectedCommits = List("5fd36592a26b07fdb58e79e4efbb6b70daea54df",
      "a7fbc74335c2df27002e8158f8e83a919195eed7")
    val numAdded = results.filter(_.add).size
    val numRemoved = results.filter(x => !x.add).size
    commits should contain theSameElementsAs expectedCommits
    numAdded should be (23)
    numRemoved should be (3)
    results should contain (PatchRecord("5fd36592a26b07fdb58e79e4efbb6b70daea54df",
      399, 398, 4,
      "    // If the data is already approriately partioned with a known partioner we can work locally.",
      "core/src/main/scala/org/apache/spark/rdd/RDD.scala",
      false))
    results should contain (PatchRecord("5fd36592a26b07fdb58e79e4efbb6b70daea54df",
      399, 399, 5,
      "    // If the data is already approriately partitioned with a known partitioner we can work locally.",
      "core/src/main/scala/org/apache/spark/rdd/RDD.scala",
      true))

  }
}
