package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * A simple test for fetching github patches
 */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class DataFetchTest extends FunSuite with SharedSparkContext {
  test("calling with a local file fetches a result") {
    // TODO
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(List(
      "pull_request_url,pull_patch_url,comments_positions_space_delimited,comments_original_positions_space_delimited",
      "https://api.github.com/repos/mick-warehime/sixth_corp/pulls/61,https://github.com/mick-warehime/sixth_corp/pull/61.patch,4 37 35 35 38 4 37 35 38,4 37 35 35 38 4 37 35 38"
    ))
    val inputData = session.read.option("header", "true").csv(session.createDataset(inputRDD)).as[InputData]
    val cachedData = session.emptyDataset[StoredPatch]
    val dataFetch = new DataFetch(sc)
    val result = dataFetch.fetchPatches(inputData, cachedData)
    result.count() should be (1)
  }
  test("cache records are filtered out") {
    // TODO
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(List(
      "pull_request_url,pull_patch_url,comments_positions_space_delimited,comments_original_positions_space_delimited",
      "https://api.github.com/repos/mick-warehime/sixth_corp/pulls/61,https://github.com/mick-warehime/sixth_corp/pull/61.patch,4 37 35 35 38 4 37 35 38,4 37 35 35 38 4 37 35 38"
    ))
    val inputData = session.read.option("header", "true").csv(session.createDataset(inputRDD)).as[InputData]
    val basicCached = StoredPatch(
      "https://api.github.com/repos/mick-warehime/sixth_corp/pulls/61",
      "notreal")
    val cachedData = session.createDataset(sc.parallelize(List(basicCached)))
    val dataFetch = new DataFetch(sc)
    val result = dataFetch.fetchPatches(inputData, cachedData)
    result.count() should be (0)
  }
}
