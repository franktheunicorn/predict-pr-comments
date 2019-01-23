package com.holdenkarau.predict.pr.comments.sparkProject.ml
/**
 * A simple test to make sure an individual model can be read through
 */
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.ResultData

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class BasicE2EModelTest extends FunSuite with SharedSparkContext {
  test("empty smoke test") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val input = session.emptyDataset[ResultData]
    val trainer = new TrainingPipeline(sc)
    val result = trainer.trainModel(input)
  }
}
