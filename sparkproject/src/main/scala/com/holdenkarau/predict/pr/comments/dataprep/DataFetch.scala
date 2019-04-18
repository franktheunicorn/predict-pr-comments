package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

/**
 * Fetching the big query comment results
 */

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
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
    val cleanedInputData = cleanInputs(inputData)
    cleanedInputData.write.format("parquet").mode(SaveMode.Append).save(output)
  }

  def createCSVReader() = {
    session.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
  }

  def createReader() = {
    session.read.format("parquet")
  }

  def loadInput(input: String) = {
    // Use default parallelism for the input because the other values
    // do it based on the input layout and our input is not well partitioned.
    val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt
    createReader().load(input).repartition(inputParallelism)
  }

  def loadInput(input: Dataset[String]) = {
    createCSVReader.csv(input)
  }


  def cleanInputs(inputData: Dataset[CommentInputData]): Dataset[ParsedCommentInputData] = {
    // Filter out bad records
    val filteredInput = inputData.na.drop("any",
      List("pull_request_url"))
      .filter(!($"pull_request_url" === "null"))

    // Strip out the start end "s
    val processPathsUDF = udf(DataFetch.processPaths _)

    val cleanedInputData = filteredInput.select(
      filteredInput("pull_request_url"),
      filteredInput("pull_patch_url"),
      filteredInput("comment_positions"),
      filteredInput("diff_hunks"),
      processPathsUDF(filteredInput("comment_file_path")).alias("comment_file_paths"),
      filteredInput("comment_commit_ids")).as[ParsedCommentInputData]
    cleanedInputData
  }

}

object DataFetch {
  def processPaths(input: Seq[String]): Seq[String] = {
    input.map(_.replaceAll("^\"|\"$", ""))
  }
}
