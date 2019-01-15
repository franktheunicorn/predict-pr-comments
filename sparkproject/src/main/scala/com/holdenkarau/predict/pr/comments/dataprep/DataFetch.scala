package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Fetch the patches from GitHub
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.hadoop.fs.{FileSystem => HDFileSystem, Path => HDPath}

import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.future._

import scala.concurrent._
import scala.concurrent.duration.Duration


case class StoredPatch(pull_request_url: String, patch: String)
case class InputData(pull_request_url: String,
  pull_patch_url: String,
  comments_positions_space_delimited: String,
  comments_original_positions_space_delimited: String)
case class ResultData(
  pull_request_url: String,
  pull_patch_url: String,
  comments_positions_space_delimited: String,
  comments_original_positions_space_delimited: String,
  patch: String)


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
    val patches = fetchPatches(inputData, cachedData)
    cache match {
      case Some(x) =>
        patches.cache()
        patches.map(_._2).write.format("parquet").mode(SaveMode.Append).save(x)
      case _ => // No cahce, no problem!
    }
    val resultData = patches.map{case (input, patch) =>
      ResultData(
        input.pull_request_url,
        input.pull_patch_url,
        input.comments_positions_space_delimited,
        input.comments_original_positions_space_delimited,
        patch.patch)
    }
    resultData.write.format("parquet").mode(SaveMode.Append).save(output)
  }
  /**
   * Fetches the github PR diff's for elements not found in the cache
   * and returns the new patches.
   */
  def fetchPatches(inputData: Dataset[InputData], cachedData: Dataset[StoredPatch]):
      Dataset[(InputData, StoredPatch)] = {
    // TODO -- use the cache to filter out from inputData
    inputData.mapPartitions(DataFetch.fetchPatchesIterator)
  }
}

object DataFetch {
  // Note if fetch patch is called inside the root JVM this might result in serilization "fun"
  implicit lazy val sttpBackend = AsyncHttpClientFutureBackend()
  import scala.concurrent.ExecutionContext.Implicits.global

  def fetchPatch(record: InputData): Future[(InputData, Response[String])] = {
    val firstRequest = sttp
      .get(uri"${record.pull_patch_url}")
    val responseFuture = firstRequest.send()
    responseFuture.map(result => (record, result))
  }

  def processResponse(data: (InputData, Response[String])):
      Option[(InputData, StoredPatch)] = {
    val (input, response) = data
    // Skip errors, we have a lot of data
    response.code match {
      case StatusCodes.Ok =>
        Some((input, StoredPatch(input.pull_request_url, response.unsafeBody)))
      case _ => None
    }
  }

  def fetchPatchesIterator(records: Iterator[InputData]):
      Iterator[(InputData, StoredPatch)] = {
    val patchFutures = records.map(fetchPatch)
    val resultFutures = patchFutures.map(future => future.map(processResponse))
    val result = new BufferedFutureIterator(resultFutures).flatMap(x => x)
    result
  }
}
