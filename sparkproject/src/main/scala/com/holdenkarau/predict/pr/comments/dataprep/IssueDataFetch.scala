package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

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


class IssueDataFetch(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  // test needed
  /**
   * Fetch the github issues and extract 
   */
  def fetch(input: String,
    output: String) = {
    val rawInputData = loadInput(input)
    val inputData = rawInputData.as[IssueInputRecord]

    val resultData = processInput(inputData)
    resultData.repartition($"project").write
      .partitionBy("project")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(output)
  }

  def processInput(inputData: Dataset[IssueInputRecord]) = {
    val issues = inputData.mapPartitions(IssueDataFetch.fetchIssuesIterator)
    val resultData = issues.flatMap(IssueDataFetch.extractStackTraces)
    resultData.distinct()
    resultData
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
}

object IssueDataFetch {
  // Note if fetch patch is called inside the root JVM this might result in serilization "fun"
  @transient implicit lazy val sttpBackend = AsyncHttpClientFutureBackend()
  import scala.concurrent.ExecutionContext.Implicits.global

  def fetchIssue(record: IssueInputRecord):
      Future[(IssueInputRecord, Response[String])] = {
    try {
      val issueRequest = sttp
        .get(uri"${processPath(record.url)}")
      val issueResponseFuture = issueRequest.send()
      issueResponseFuture.map{case (result) => (record, result)}
    } catch {
      case e: Exception => // We can get null pointers and other weird errors trying to fetch
        Future.failed[(IssueInputRecord, Response[String])](e)
    }
  }

  def processResponse(data: (IssueInputRecord, Response[String])):
      Option[(IssueInputRecord, String)] = {
    val (input, response) = data
    // Skip errors, we have a lot of data
    if (response.code == StatusCodes.Ok) {
      Some((input, response.unsafeBody))
    } else {
      None
    }
  }

  def fetchIssuesIterator(records: Iterator[IssueInputRecord]):
      Iterator[(IssueInputRecord, String)] = {
    val issueFutures = records.map(fetchIssue)
    val resultFutures = issueFutures.map(future => future.map(processResponse))
    val result = new BufferedFutureIterator(resultFutures)
      .flatMap(x => x).flatMap(x => x)
    result
  }

  def processPath(input: String): String = {
    input.replaceAll("^\"|\"$", "")
  }

  def extractStackTraces(input: (IssueInputRecord, String)): Iterator[IssueStackTrace] = {
    val record: IssueInputRecord = input._1
    val inputStr = input._2
    val repo = record.name
    // TODO: better regex
    // Scala regex is jank with handling escaped () so just strip them as " "s
    val cleanedStr = inputStr.replaceAll(raw"\)", " ").replaceAll(raw"\(", " ")
    val stackTraceRegex = raw"[\s\\/]+([^ /]*?\.[^ /]*?):(\d+)\s*".r
    val stackTraces = for (m <- stackTraceRegex.findAllMatchIn(cleanedStr)) yield IssueStackTrace(repo, m.group(1), m.group(2).toInt)
    stackTraces
  }
}
