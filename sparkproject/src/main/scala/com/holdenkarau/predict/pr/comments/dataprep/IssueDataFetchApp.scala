package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to fetch the issues from github
  * sbt "run inputFile outputFile"
  *  (+ select DataFetch when prompted)
  */
/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object IssueDataFetchSCApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  IssueRunner.run(new SparkConf(), inputFile, outputFile)
}

private object IssueRunner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {

    val sc = new SparkContext(conf)
    val dataFetch = new IssueDataFetch(sc)
    dataFetch.fetch(inputFile, outputFile)
  }
}
