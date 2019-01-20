package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Fetch the patches from GitHub
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem => HDFileSystem, Path => HDPath}

import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.future._

import scala.concurrent._
import scala.concurrent.duration.Duration


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
    val rawInputData = loadInput(input)
    val inputData = rawInputData.as[InputData]
    // Check and see if we have data prom a previous run
    val fs = HDFileSystem.get(sc.hadoopConfiguration)
    val cachedData = cache match {
      case Some(x) if fs.exists(new HDPath(x)) =>
        session.read.format("parquet").load(x).as[StoredPatch]
      case _ => session.emptyDataset[StoredPatch]
    }
    val cleanedInputData = cleanInputs(inputData)

    val patches = fetchPatches(cleanedInputData, cachedData)
    cache match {
      case Some(x) =>
        patches.cache()
        patches.map(_._2).write.format("parquet").mode(SaveMode.Append).save(x)
      case _ => // No cahce, no problem!
    }

    val resultData = patches.mapPartitions{partition =>
      def processSpaceDelimCommentPos(input: List[String]): List[Option[Int]] = {
        input.map{
          case "-1" => None // Magic value for null
          case x => Some(x.toInt) // Anything besides -1 should be fine
        }.toList
      }


      def processPatch (result: (ParsedInputData, StoredPatch)): ResultData = {
        val (input, patch) = result
        ResultData(
          input.pull_request_url,
          input.pull_patch_url,
          processSpaceDelimCommentPos(input.comments_positions),
          processSpaceDelimCommentPos(input.comments_original_positions),
          input.comment_file_paths,
          input.comment_commit_ids,
          patch.patch,
          patch.diff)
      }
      partition.map(processPatch)
    }
    resultData.write.format("parquet").mode(SaveMode.Append).save(output)
  }

  def createCSVReader() = {
    session.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
  }

  def loadInput(input: String) = {
    createCSVReader().load(input)
  }

  def loadInput(input: Dataset[String]) = {
    createCSVReader.csv(input)
  }


  def cleanInputs(inputData: Dataset[InputData]): Dataset[ParsedInputData] = {
    // Strip out the "s because it's just a base64 string
    val processSpaceDelimCommitIdsUDF = udf(DataFetch.processSpaceDelimCommitIds _)

    val cleanedInputData = inputData.select(
      inputData("pull_request_url"),
      inputData("pull_patch_url"),
      split(inputData("comments_positions_space_delimited"), " ").alias(
        "comments_positions"),
      split(inputData("comments_original_positions_space_delimited"), " ").alias(
        "comments_original_positions"),
      from_json(inputData("comment_file_paths_json_encoded"),
        ArrayType(StringType)).alias("comment_file_paths"),
      processSpaceDelimCommitIdsUDF(
        split(inputData("comment_commit_ids_space_delimited"), " ")).alias(
        "comment_commit_ids")).as[ParsedInputData]
    cleanedInputData
  }

  /**
   * Fetches the github PR diff's for elements not found in the cache
   * and returns the new patches.
   */
  def fetchPatches(inputData: Dataset[ParsedInputData], cachedData: Dataset[StoredPatch]):
      Dataset[(ParsedInputData, StoredPatch)] = {
    val joinedData = inputData.join(cachedData,
      Seq("pull_request_url"),
      joinType = "left_anti")
    val result = joinedData.as[ParsedInputData].mapPartitions(
      DataFetch.fetchPatchesIterator)
    result
  }
}

object DataFetch {
  // Note if fetch patch is called inside the root JVM this might result in serilization "fun"
  implicit lazy val sttpBackend = AsyncHttpClientFutureBackend()
  import scala.concurrent.ExecutionContext.Implicits.global

  def fetchPatch(record: ParsedInputData):
      Future[(ParsedInputData, Response[String], Response[String])] = {
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

  def processResponse(data: (ParsedInputData, Response[String], Response[String])):
      Option[(ParsedInputData, StoredPatch)] = {
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

  def fetchPatchesIterator(records: Iterator[ParsedInputData]):
      Iterator[(ParsedInputData, StoredPatch)] = {
    val patchFutures = records.map(fetchPatch)
    val resultFutures = patchFutures.map(future => future.map(processResponse))
    val result = new BufferedFutureIterator(resultFutures).flatMap(x => x)
    result
  }

  def processSpaceDelimCommitIds(input: Seq[String]): Seq[String] = {
    input.map(_.replaceAll("\"", ""))
  }

}
