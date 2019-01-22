package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Fetch the patches from GitHub
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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
    val cachedData = cache match {
      case Some(x) =>
        try {
          session.read.format("parquet").load(x).as[StoredPatch]
        } catch {
          case _ => session.emptyDataset[StoredPatch]
        }
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
    // Use default parallelism for the input because the other values
    // do it based on the input layout and our input is not well partitioned.
    val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt
    createCSVReader().load(input).repartition(inputParallelism)
  }

  def loadInput(input: Dataset[String]) = {
    createCSVReader.csv(input)
  }


  def cleanInputs(inputData: Dataset[InputData]): Dataset[ParsedInputData] = {
    // Filter out bad records
    val filteredInput = inputData.na.drop("any",
      List("pull_request_url", "pull_patch_url"))
      .filter(!($"pull_request_url" === "null"))
      .filter(!($"pull_patch_url" === "null"))

    // Strip out the "s because it's just a base64 string
    val processSpaceDelimCommitIdsUDF = udf(DataFetch.processSpaceDelimCommitIds _)
    // Strip out the start end "s
    val processPathsUDF = udf(DataFetch.processPaths _)

    val cleanedInputData = filteredInput.select(
      filteredInput("pull_request_url"),
      filteredInput("pull_patch_url"),
      split(filteredInput("comments_positions_space_delimited"), " ").alias(
        "comments_positions"),
      split(filteredInput("comments_original_positions_space_delimited"), " ").alias(
        "comments_original_positions"),
      processPathsUDF(from_json(filteredInput("comment_file_paths_json_encoded"),
        ArrayType(StringType))).alias("comment_file_paths"),
      processSpaceDelimCommitIdsUDF(
        split(filteredInput("comment_commit_ids_space_delimited"), " ")).alias(
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
  @transient implicit lazy val sttpBackend = AsyncHttpClientFutureBackend()
  import scala.concurrent.ExecutionContext.Implicits.global

  def fetchPatch(record: ParsedInputData):
      Future[(ParsedInputData, Response[String], Response[String])] = {
    try {
      val patchRequest = sttp
        .get(uri"${record.pull_patch_url}")
      val patchResponseFuture = patchRequest.send()
      val diffUrl = record.pull_patch_url.substring(0, record.pull_patch_url.length - 5) + "diff"
      val diffRequest = sttp
        .get(uri"${diffUrl}")
      val diffResponseFuture = diffRequest.send()
      val responseFuture = patchResponseFuture.zip(diffResponseFuture)
      responseFuture.map{case (patch, diff) => (record, patch, diff)}
    } catch {
      case e: Exception => // We can get null pointers and other weird errors trying to fetch
        Future.failed[(ParsedInputData, Response[String], Response[String])](e)
    }
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
    val result = new BufferedFutureIterator(resultFutures)
      .flatMap(x => x).flatMap(x => x)
    result
  }

  def processSpaceDelimCommitIds(input: Seq[String]): Seq[String] = {
    input.map(_.replaceAll("\"", ""))
  }

  def processPaths(input: Seq[String]): Seq[String] = {
    input.map(_.replaceAll("^\"|\"$", ""))
  }
}
