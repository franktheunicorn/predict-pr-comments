package com.holdenkarau.predict.pr.comments.sparkProject.ml

/**
 * A simple test to make sure that we produce the correct labeled features
 */
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.ResultCommentData

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalatest.FunSuite
import org.scalatest.Matchers._
/*
class LabeledFeatureTest extends FunSuite with SharedSparkContext {
  test("test we extract the correct labeled features") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val schema = ScalaReflection.schemaFor[ResultData].dataType.asInstanceOf[StructType]
    val input = session.read.schema(schema).format("json").json(
      sc.parallelize(List(E2EModelSampleRecord.record))).as[ResultData]
    val labeledRecords = input.flatMap(TrainingPipeline.produceRecord)
    val localRecords = labeledRecords.collect()
    labeledRecords.filter($"commented" === true).collect() should contain (
      LabeledRecord("	metadata.SourceIP = parseSourceIP(conn)","adapters/inbound/http.go",true,true, 34))
  }
}
 */
