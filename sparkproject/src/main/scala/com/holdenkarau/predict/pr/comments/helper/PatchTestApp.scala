package com.holdenkarau.predict.pr.comments.sparkProject.helper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
 * An app to run patch extraction and save invalid records in JSON for inspection.
 */
object PatchTestAppSC extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  val sc = new SparkContext(new SparkConf())
  val session = SparkSession.builder.getOrCreate()
  import session.implicits._
  // Use default parallelism for the input because the other values
  // do it based on the input layout and our input is not well partitioned.
  val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt
  val input = session.read.format("parquet").option("mergeSchema", "true").load(inputFile).repartition(inputParallelism)
  input.cache()
  input.count()
  val rejected = input.select("patch").as[String].flatMap {patch => 
    try {
      PatchExtractor.processPatch(patch)
      None
    } catch {
      case e: Exception => Some((patch, e.toString))
    }
  }
  rejected.write.format("json").save(outputFile)
}
