package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * 
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.hadoop.fs.{FileSystem => HDFileSystem, Path => HDPath}

case class StoredPatch(pr: String, patch: String)
case class InputData(pull_request_url: String,
  pull_patch_url: String,
  comments_positions_space_delimited: String,
  comments_original_positions_space_delimited: String)

class DataFetch(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  /**
   * Fetch the github PR diffs
   */
  def fetch(sc: SparkContext,
    input: String,
    output: String,
    cache: Option[String]): Unit = {
    val rawInputData = session.read.format("csv").option("header", "true").option("inferSchema", "true").load(input)
    val inputData = rawInputData.as[InputData]
    // Check and see if we have data prom a previous run
    val fs = HDFileSystem.get(sc.hadoopConfiguration)
    val cachedData = cache match {
      case Some(x) if fs.exists(new HDPath(x)) =>
        session.read.format("parquet").load(x).as[StoredPatch]
      case _ => session.emptyDataset[StoredPatch]
    }
    val result = processInput(inputData, cachedData)
  }
  /**
   * Fetches the github PR diff's for elements not found in the cache
   * and returns the new patches.
   */
  def processInput(inputData: Dataset[InputData], cachedData: Dataset[StoredPatch]):
      Dataset[StoredPatch] = {
    session.emptyDataset[StoredPatch]
  }
}
