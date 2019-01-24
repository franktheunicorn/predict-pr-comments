package com.holdenkarau.predict.pr.comments.sparkProject.ml

/**
 * A simple test to make sure an individual model can be trained
 */
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.ResultData

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class BasicE2EModelTest extends FunSuite with SharedSparkContext {
  test("tiny smoke test") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val schema = ScalaReflection.schemaFor[ResultData].dataType.asInstanceOf[StructType]
    val input = session.read.schema(schema).format("json").json(
      sc.parallelize(List(E2EModelSampleRecord.record))).as[ResultData]
    val trainer = new TrainingPipeline(sc)
    val pipelineModel = trainer.trainModel(input)
    val transformedResult = pipelineModel.transform(
      trainer.prepareTrainingData(input))
  }
}
