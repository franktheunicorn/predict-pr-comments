package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Fetch the patches from GitHub
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem => HDFileSystem, Path => HDPath}

import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.future._

import scala.concurrent._
import scala.concurrent.duration.Duration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference


case class StoredPatch(pull_request_url: String, patch: String, diff: String)
case class InputData(
  pull_request_url: String,
  pull_patch_url: String,
  comments_positions_space_delimited: String,
  comments_original_positions_space_delimited: String,
  comment_file_paths_json_encoded: String,
  comment_commit_ids_space_delimited: String)
case class ResultData(
  pull_request_url: String,
  pull_patch_url: String,
  // Some comments might not resolve inside of the updated or original spaces
  // hence the Option type.
  comments_positions_space: List[Option[Int]],
  comments_original_positions_space_delimited: List[Option[Int]],
  comment_file_paths: List[String],
  comment_commit_ids: List[String],
  patch: String,
  diff: String)


class DataFetch(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  // test needed
  /**
   * Fetch the github PR diffs
   */
  def fetch(input: String,
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

    val resultData = patches.mapPartitions{partition =>
      // Todo move to the companion object + lazy val
      val mapper = new ObjectMapper()
      def processSpaceDelimCommentPos(input: String): List[Option[Int]] = {
        input.split(" ").map{
          case "-1" => None // Magic value for null
          case x => Some(x.toInt) // Anything besides -1 should be fine
        }.toList
      }
      def processPatch (result: (InputData, StoredPatch)): ResultData = {
        val (input, patch) = result
        ResultData(
          input.pull_request_url,
          input.pull_patch_url,
          processSpaceDelimCommentPos(input.comments_positions_space_delimited),
          processSpaceDelimCommentPos(input.comments_original_positions_space_delimited),
          mapper.readValue[Array[String]](input.comment_file_paths_json_encoded, classOf[Array[String]]).toList,
          input.comment_commit_ids_space_delimited.split(" ").toList.map(removeQoutes),
          patch.patch,
          patch.diff)
      }
      partition.map(processPatch)
    }
    resultData.write.format("parquet").mode(SaveMode.Append).save(output)
  }
  /**
   * Fetches the github PR diff's for elements not found in the cache
   * and returns the new patches.
   */
  def fetchPatches(inputData: Dataset[InputData], cachedData: Dataset[StoredPatch]):
      Dataset[(InputData, StoredPatch)] = {
    val joinedData = inputData.join(cachedData,
      Seq("pull_request_url"),
      joinType = "left_anti")
    val result = joinedData.as[InputData].mapPartitions(DataFetch.fetchPatchesIterator)
    result
  }
}

object DataFetch {
  // Note if fetch patch is called inside the root JVM this might result in serilization "fun"
  implicit lazy val sttpBackend = AsyncHttpClientFutureBackend()
  import scala.concurrent.ExecutionContext.Implicits.global

  def fetchPatch(record: InputData): Future[(InputData, Response[String], Response[String])] = {
    val patchRequest = sttp
      .get(uri"${record.pull_patch_url}")
    val patchResponseFuture = patchRequest.send()
    val diffUrl = record.pull_patch_url.substring(0, record.pull_patch_url.length - 5) + "diff"
    val diffRequest = sttp
      .get(uri"${diffUrl}")
    val diffResponseFuture = diffRequest.send()
    val responseFuture = patchResponseFuture.zip(diffResponseFuture)
    responseFuture.map{case (patch, diff) => (record, patch, diff)}
  }

  def processResponse(data: (InputData, Response[String], Response[String])):
      Option[(InputData, StoredPatch)] = {
    val (input, patchResponse, diffResponse) = data
    // Skip errors, we have a lot of data
    if (patchResponse.code == StatusCodes.Ok && diffResponse.code == StatusCodes.Ok) {
      Some((input,
        StoredPatch(
          input.pull_request_url,
          patchResponse.unsafeBody,
          diffResponse.unsafeBody)))
    } else {
      None
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
