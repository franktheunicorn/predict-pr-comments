package com.holdenkarau.predict.pr.comments.sparkProject.helper

import org.apache.spark.sql._

/**
 * An app to run patch extraction and save invalid records in JSON for inspection.
 */
object PatchTestAppSC extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  val session = SparkSession.builder.getOrCreate()
  import session.implicits._
  val input = session.read.format("parquet").load(inputFile).select("patch").as[String].repartition(40)
  input.cache()
  input.count()
  val rejected = input.flatMap {patch => 
    try {
      PatchExtractor.processPatch(patch)
      None
    } catch {
      case e: Exception => Some((patch, e.toString))
    }
  }
  rejected.write.format("json").save(outputFile)
}
