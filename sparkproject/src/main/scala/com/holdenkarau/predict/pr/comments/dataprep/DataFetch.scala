package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

/**
 * Fetching the big query comment results
 */

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



class DataFetch(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  // test needed
  /**
   * Fetch the big query comments
   */
  def fetch(input: String,
    output: String,
    cache: Option[String]): Unit = {
    val rawInputData = loadInput(input)
    val inputData = rawInputData.as[CommentInputData]
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

    val patchFetcher = new PatchFetcher(sc)

    val patches = patchFetcher.fetchPatches(cleanedInputData, cachedData)
    cache match {
      case Some(x) =>
        patches.cache()
        patches.map(_._2).write.format("parquet").mode(SaveMode.Append).save(x)
      case _ => // No cahce, no problem!
    }

    val resultData = patches.mapPartitions{partition =>
      def processPatch (result: (ParsedCommentInputData, StoredPatch)): ResultCommentData = {
        val (input, patch) = result
        ResultCommentData(
          input.pull_request_url,
          input.pull_patch_url,
          input,
          patch.patch,
          patch.diff)
      }
      partition.map(processPatch)
    }
    resultData.write.format("parquet").mode(SaveMode.Append).save(output)
  }

  val inputSchema = ScalaReflection.schemaFor[CommentInputData]
    .dataType.asInstanceOf[StructType]

  def createJSONReader() = {
    session.read.format("json")
      .schema(inputSchema)
  }

  def createReader() = {
    session.read.format("parquet")
      .schema(inputSchema)
  }

  def loadInput(input: String) = {
    // Use default parallelism for the input because the other values
    // do it based on the input layout and our input is not well partitioned.
    val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt
    createReader().load(input).repartition(inputParallelism)
  }

  def loadJsonInput(input: Dataset[String]) = {
    createJSONReader.json(input)
  }


  def cleanInputs(inputData: Dataset[CommentInputData]): Dataset[ParsedCommentInputData] = {
    // Filter out bad records
    val filteredInput = inputData.na.drop("any",
      List("pull_request_url"))
      .filter(!($"pull_request_url" === "null"))

    // Strip out the start end "s
    val processPathsUDF = udf(DataFetch.processPaths _)

    // Strip out the start end "s
    val processUrlUDF = udf(DataFetch.processUrl _)

    // Strip out all the "s
    val processCommitIdsUDF = udf(DataFetch.processCommitIds _)

    // Strip out all the "s
    val processDiffHunksUDF = udf(DataFetch.processDiffHunks _)

    val cleanedInputData = filteredInput.select(
      processUrlUDF(filteredInput("pull_request_url"))
        .alias("pull_request_url"),
      processUrlUDF(filteredInput("pull_patch_url"))
        .alias("pull_patch_url"),
      filteredInput("comment_positions"),
      filteredInput("comment_text"),
      processDiffHunksUDF(filteredInput("diff_hunks")).alias("diff_hunks"),
      processPathsUDF(filteredInput("comment_file_paths")).alias("comment_file_paths"),
      processCommitIdsUDF(filteredInput("comment_commit_ids")).alias("comment_commit_ids")
    ).as[ParsedCommentInputData]
    cleanedInputData
  }

}

object DataFetch {

  def processUrl(input: String): String = {
    input.replaceAll("^\"|\"$", "")
  }

  def processPaths(input: Seq[String]): Seq[String] = {
    input.map(_.replaceAll("^\"|\"$", ""))
  }

  def processDiffHunks(input: Seq[String]): Seq[String] = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    input.map(elem =>
      mapper.readValue[String](elem)
    )
  }

  def processCommitIds(input: Seq[String]): Seq[String] = {
    input.map(_.replaceAll("\"", ""))
  }
}
