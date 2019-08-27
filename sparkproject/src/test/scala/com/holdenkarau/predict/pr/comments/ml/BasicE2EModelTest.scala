package com.holdenkarau.predict.pr.comments.sparkProject.ml

/**
 * A simple test to make sure an individual model can be trained
 */
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep._

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class BasicE2EModelTest extends FunSuite with SharedSparkContext {
  test("tiny smoke test") {
    val tempDir = Utils.createTempDir()
    val dataprepModelTempPath = tempDir.toPath().toAbsolutePath().toString()

    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val schema = ScalaReflection.schemaFor[CommentInputData].dataType.asInstanceOf[StructType]
    val input = session.read.schema(schema).format("json").json(
      sc.parallelize(List(E2EModelSampleRecord.record))).as[CommentInputData]
    input.show()
    val fetcher = new DataFetch(sc)
    val cleaned = fetcher.cleanInputs(input)
    val parsed = fetcher.innerFetch(cleaned, session.emptyDataset[StoredPatch], None)
    val issues = session.emptyDataset[IssueStackTrace]
    val trainer = new TrainingPipeline(sc)
    val pipelineModel = trainer.trainModel(parsed, issues, dataprepModelTempPath)
    val featurizer = new Featurizer(sc)
    val transformedResult = pipelineModel.transform(
      featurizer.prepareTrainingData(parsed, issues))
  }
/*
  test("tiny train and fit smoke test") {
    val tempDir = Utils.createTempDir()
    val dataprepModelTempPath = tempDir.toPath().toAbsolutePath().toString()

    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val schema = ScalaReflection.schemaFor[ResultCommentData].dataType.asInstanceOf[StructType]
    val input = session.read.schema(schema).format("json").json(
      sc.parallelize(List(E2EModelSampleRecord.record))).as[ResultCommentData]
    // TODO: Add issues test
    val issues = session.emptyDataset[IssueStackTrace]
    // Make copies of the data so we can have a test set
    // Note: means our results are kind of BS but it's just for testing
    val synth = input.flatMap(x => List.fill(5)(x))
    val trainer = new TrainingPipeline(sc)
    val (pipelineModel, prScore, rocScore, datasetSize, positives) =
      trainer.trainAndEvalModel(synth, issues, split=List(0.5, 0.5), fast=true,
        dataprepPipelineLocation=dataprepModelTempPath)
    datasetSize should be > (positives)
  }
 */
}
