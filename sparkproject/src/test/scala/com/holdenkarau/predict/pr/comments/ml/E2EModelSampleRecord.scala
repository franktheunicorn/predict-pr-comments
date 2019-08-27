package com.holdenkarau.predict.pr.comments.sparkProject.ml
object E2EModelSampleRecord {
  // This is designed to be a 
  // Note this record is modified to have the properties we want to test for.
  val record = ("""[
  {
    "pull_request_url": "\"https://api.github.com/repos/apache/spark/pulls/25515\"",
    "pull_patch_url": "\"https://github.com/apache/spark/pull/25515.patch\"",
    "created_date": "\"2019-08-21T00:34:46Z\"",
    "diff_hunks": [
      "\"@@ -520,11 +520,14 @@ def collect(self):\\n \\n     @ignore_unicode_prefix\\n     @since(2.0)\\n-    def toLocalIterator(self):\\n+    def toLocalIterator(self, prefetchPartitions=False):\"",
      "\"@@ -520,11 +520,14 @@ def collect(self):\\n \\n     @ignore_unicode_prefix\\n     @since(2.0)\\n-    def toLocalIterator(self):\\n+    def toLocalIterator(self, prefetchPartitions=False):\"",
      "\"@@ -196,10 +204,15 @@ private[spark] object PythonRDD extends Logging {\\n           // Read request for data, value of zero will stop iteration or non-zero to continue\\n           if (in.readInt() == 0) {\\n             complete = true\\n-          } else if (collectPartitionIter.hasNext) {\\n+          } else if (prefetchIter.hasNext) {\\n \\n             // Client requested more data, attempt to collect the next partition\\n-            val partitionArray = collectPartitionIter.next()\\n+            val partitionFuture = prefetchIter.next()\\n+            // Cause the next job to be submitted if prefecthPartitions is enabled.\\n+            if (prefetchPartitions) {\\n+              prefetchIter.headOption\\n+            }\\n+            val partitionArray = ThreadUtils.awaitResult(partitionFuture, Duration.Inf)\"",
      "\"@@ -68,6 +70,27 @@ def test_to_localiterator(self):\\n         it2 = rdd2.toLocalIterator()\\n         self.assertEqual([1, 2, 3], sorted(it2))\\n \\n+    def test_to_localiterator_prefetch(self):\\n+        # Test that we fetch the next partition in parallel\\n+        # We do this by returning the current time and:\\n+        # reading the first elem, waiting, and reading the second elem\\n+        # If not in parallel then these would be at different times\\n+        # But since they are being computed in parallel we see the time\\n+        # is \\\"close enough\\\" to the same.\\n+        rdd = self.sc.parallelize(range(2), 2)\\n+        times1 = rdd.map(lambda x: datetime.now())\\n+        times2 = rdd.map(lambda x: datetime.now())\\n+        timesIterPrefetch = times1.toLocalIterator(prefetchPartitions=True)\\n+        timesIter = times2.toLocalIterator(prefetchPartitions=False)\\n+        timesPrefetchHead = next(timesIterPrefetch)\\n+        timesHead = next(timesIter)\\n+        time.sleep(2)\\n+        timesNext = next(timesIter)\\n+        timesPrefetchNext = next(timesIterPrefetch)\\n+        print(\\\"With prefetch times are: \\\" + str(timesPrefetchHead) + \\\",\\\" + str(timesPrefetchNext))\\n+        self.assertTrue(timesNext - timesHead >= timedelta(seconds=2))\\n+        self.assertTrue(timesPrefetchNext - timesPrefetchHead < timedelta(seconds=1))\"",
      "\"@@ -0,0 +1,86 @@\\n+#\"",
      "\"@@ -68,6 +70,27 @@ def test_to_localiterator(self):\\n         it2 = rdd2.toLocalIterator()\\n         self.assertEqual([1, 2, 3], sorted(it2))\\n \\n+    def test_to_localiterator_prefetch(self):\\n+        # Test that we fetch the next partition in parallel\\n+        # We do this by returning the current time and:\\n+        # reading the first elem, waiting, and reading the second elem\\n+        # If not in parallel then these would be at different times\\n+        # But since they are being computed in parallel we see the time\\n+        # is \\\"close enough\\\" to the same.\\n+        rdd = self.sc.parallelize(range(2), 2)\\n+        times1 = rdd.map(lambda x: datetime.now())\\n+        times2 = rdd.map(lambda x: datetime.now())\\n+        timesIterPrefetch = times1.toLocalIterator(prefetchPartitions=True)\""
    ],
    "comment_positions": [
      {
        "original_position": "5",
        "new_position": "5"
      },
      {
        "original_position": "5",
        "new_position": "5"
      },
      {
        "original_position": "47",
        "new_position": "47"
      },
      {
        "original_position": "37",
        "new_position": "37"
      },
      {
        "original_position": "1",
        "new_position": "1"
      },
      {
        "original_position": "28",
        "new_position": "28"
      }
    ],
    "comment_text": [
      "\"Good catch :)\"",
      "\"looks like you miss to pass prefetchPartitions to self._jdf.toPythonIterator() below?\"",
      "\"So the awaitFuture (or something similar) is required for us to use futures. If we just used a buffered iterator without allowing the job to schedule separately we'd just block for both partitions right away instead of evaluating the other future in the background while we block on the first. (Implicitly this awaitResult is already effectively done inside of the previous DAGScheduler's runJob.\"",
      "\"I think we could if we used a fresh SparkContext but with the reused context I'm not sure how I'd know if the job was run or not.\"",
      "\"I think examples in this directory target to show how the feature or API is used rather than showing the perf results .. \\r\\nVirtually the example seems it has to be just `.toLocalIterator(prefetchPartitions=False)` which I don't think worth as a separate example file.\"",
      "\"Shall we stick to underscore naming rule?\""
    ],
    "comment_commit_ids": [
      "\"c477fec27618acea2864dbb24fda58b5736af86b\"",
      "\"c477fec27618acea2864dbb24fda58b5736af86b\"",
      "\"e0327a24e48c4ba7a483ef2590c9dd72c6bedfc5\"",
      "\"e0327a24e48c4ba7a483ef2590c9dd72c6bedfc5\"",
      "\"e0327a24e48c4ba7a483ef2590c9dd72c6bedfc5\"",
      "\"e0327a24e48c4ba7a483ef2590c9dd72c6bedfc5\""
    ],
    "comment_file_paths": [
      "\"python/pyspark/sql/dataframe.py\"",
      "\"python/pyspark/sql/dataframe.py\"",
      "\"core/src/main/scala/org/apache/spark/api/python/PythonRDD.scala\"",
      "\"python/pyspark/tests/test_rdd.py\"",
      "\"examples/src/main/python/prefetch.py\"",
      "\"python/pyspark/tests/test_rdd.py\""
    ]
  }
]""")}
