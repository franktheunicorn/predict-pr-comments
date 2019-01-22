package com.holdenkarau.predict.pr.comments.sparkProject.helper

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this grab a head from a parquet table for quick debugging
  * sbt "run inputFile outputFile escapedOutputFile"
  *  (+ select DataFetch when prompted)
  */
object JsonApp extends App{
  val (inputFile, outputFile, escapedOutputFile) = (args(0), args(1), args(2))
  val conf = new SparkConf()
    .setMaster("local")

  MyRunner.run(conf, inputFile, outputFile, escapedOutputFile)
}

object JsonSCApp extends App{
  val (inputFile, outputFile, escapedOutputFile) = (args(0), args(1), args(2))
  val conf = new SparkConf()

  MyRunner.run(conf, inputFile, outputFile, escapedOutputFile)
}

private object MyRunner {
  def run(conf: SparkConf, inputFile: String, outputFile: String,
    escapedOutputFile: String): Unit = {

    val sc = new SparkContext(conf)
    val jsonDump = new JsonDump(sc)
    jsonDump.dump(inputFile, outputFile, escapedOutputFile)
  }
}
