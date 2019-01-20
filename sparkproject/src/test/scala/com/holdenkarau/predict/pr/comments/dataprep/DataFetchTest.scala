package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * A simple test for fetching github patches
 */

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class DataFetchTest extends FunSuite with SharedSparkContext {
  val standardInputList = List(
    """pull_request_url,pull_patch_url,comments_positions_space_delimited,comments_original_positions_space_delimited,comment_file_paths_json_encoded,comment_commit_ids_space_delimited""",
    "https://api.github.com/repos/Dreamacro/clash/pulls/96,https://github.com/Dreamacro/clash/pull/96.patch,42 -1,42 42,\"[\"\"\\\"\"rules/from_ipcidr.go\\\"\"\"\",\"\"\\\"\"rules/from_ipcidr.go\\\"\"\"\"]\",\"\"\"de976981dff604f3f41167012ddb82b3e0c90e6d\"\" \"\"0b44c7a83aa400caf5db40975a75428682431309\"\"\"")

  test("Cleaning input should work") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList)
    val inputData = session.read.option("header", "true").option("escape", "\"").csv(session.createDataset(inputRDD)).as[InputData]
    val dataFetch = new DataFetch(sc)
    val cleanedInputData = dataFetch.cleanInputs(inputData).collect()(0)

    inputData.collect()(0).comment_file_paths_json_encoded should be (
    """["\"rules/from_ipcidr.go\"","\"rules/from_ipcidr.go\""]""")
    cleanedInputData.comment_commit_ids should contain theSameElementsAs List(
      "de976981dff604f3f41167012ddb82b3e0c90e6d", "0b44c7a83aa400caf5db40975a75428682431309")
    cleanedInputData.comment_file_paths should contain theSameElementsAs List(
      "\"rules/from_ipcidr.go\"", "\"rules/from_ipcidr.go\"")
  }

  test("calling with a local file fetches a result") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList)
    val inputData = session.read.option("header", "true").option("escape", "\"").csv(session.createDataset(inputRDD)).as[InputData]
    val dataFetch = new DataFetch(sc)
    val cleanedInputData = dataFetch.cleanInputs(inputData)
    val cachedData = session.emptyDataset[StoredPatch]
    val result = dataFetch.fetchPatches(cleanedInputData, cachedData)
    result.count() should be (1)
  }

  test("cache records are filtered out") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList)
    val inputData = session.read.option("header", "true").option("escape", "\"").csv(session.createDataset(inputRDD)).as[InputData]
    val basicCached = StoredPatch(
      "https://api.github.com/repos/Dreamacro/clash/pulls/96",
      "notreal",
      "stillnotreal")
    val cachedData = session.createDataset(sc.parallelize(List(basicCached)))
    val dataFetch = new DataFetch(sc)
    val cleanedInputData = dataFetch.cleanInputs(inputData)
    val result = dataFetch.fetchPatches(cleanedInputData, cachedData)
    result.count() should be (0)
  }

  test("test the main entry point - no cache") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList, 1)
    val inputPath = s"$tempPath/input.csv"
    val outputPath = s"$tempPath/output.csv"
    inputRDD.saveAsTextFile(inputPath)
    val dataFetch = new DataFetch(sc)
    dataFetch.fetch(inputPath, outputPath, None)
    val result = session.read.format("parquet").load(outputPath)
    result.count() should be (1)
  }

  test("test the main entry point - with cache") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList, 1)
    val inputPath = s"$tempPath/input.csv"
    val outputPath = s"$tempPath/output.csv"
    val cachePath = s"$tempPath/cache"
    inputRDD.saveAsTextFile(inputPath)
    val dataFetch = new DataFetch(sc)
    dataFetch.fetch(inputPath, outputPath, Some(cachePath))
    dataFetch.fetch(inputPath, outputPath, Some(cachePath))
    val result = session.read.format("parquet").load(outputPath)
    result.count() should be (1)
    result.as[ResultData].collect()(0).patch should include ("Subject: [PATCH")
    result.as[ResultData].collect()(0).diff should include ("@@ -")
    result.as[ResultData].collect()(0).diff should not include ("Subject: [PATCH")
  }

}
