package com.holdenkarau.predict.pr.comments.sparkProject.helper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


class JsonDump(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  def dump(inputFile: String, outputFile: String, escapedOutputFile: String): Unit = {
    val input = session.read.format("parquet").load(inputFile).limit(50)
    input.write.format("json").save(outputFile)
    val rawJson = sc.textFile(outputFile)
    import scala.reflect.runtime.universe._
    val jsonEscaped = rawJson.map{record => Literal(Constant(record)).toString}
    jsonEscaped.saveAsTextFile(escapedOutputFile)
  }
}
