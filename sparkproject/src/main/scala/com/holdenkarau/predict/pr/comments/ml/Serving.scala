package com.holdenkarau.predict.pr.comments.sparkProject.ml

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}

import suggester.service.suggester._
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep._
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor
import com.softwaremill.sttp.Response

class ModelServingService extends ModelRequestGrpc.ModelRequest {
  import scala.concurrent.ExecutionContext.Implicits.global
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._
  val issueSchema = ScalaReflection.schemaFor[IssueStackTrace].dataType.asInstanceOf[StructType]
  // We don't strongly type here because of query push down fuzzyness
  val issueData = session.read.format("parquet").schema(issueSchema).load(ModelServingService.issueDataLocation)

  val model = PipelineModel.load(ModelServingService.pipelineLocation)

  override def getComment(request: GetCommentRequest) = {
    val pullRequestPatchURL = request.pullRequestPatchURL
    val diffFuture = DataFetch.fetchDiffForPatchUrl(pullRequestPatchURL)
    diffFuture.map(diff => predictOnResponse(request, diff))
  }

  def predictOnResponse(request: GetCommentRequest, diffResponse: Response[String]):
      GetCommentResponse = {
    // TODO(holden): check the status processResponse
    //if (diffResponse.code == StatusCodes.Ok) {
    //}
    val diff = diffResponse.unsafeBody
    val patchRecords = PatchExtractor.processPatch(patch=diff, diff=true)
    val elems = patchRecords.map{record =>
      val extension = TrainingPipeline.extractExtension(record.filename).getOrElse(null)
      val oldPos = if (record.oldPos == null) {
        -1
      } else {
        record.oldPos
      }
      val newPos = if (record.newPos == null) {
        -1
      } else {
        record.newPos
      }

      val foundIssueCount = issueData
        .filter($"project" === request.repoName)
        .filter($"filename" === record.filename)
        .filter($"line" === oldPos)
        .count()
      PreparedData(
        text=record.text,
        filename=record.filename,
        add=record.add,
        // TODO: Hack fix this in training pipeline later
        commented=newPos.toDouble,// commented, stripped anyways -- Going to encode some info I needed oops
        extension=extension,
        line_length=record.text.length,
        label=0.0, // label stripped anyways
        only_spaces = record.text match {
          case ModelServingService.onlySpacesRegex(_*) => 1.0
          case _ => 0.0
        },
        not_in_issues = foundIssueCount match {
          case 0 => 1.0
          case _ => 0.0
        } // Not in issues
      )
    }
    val elemsDF = session.createDataFrame(elems)
    val predictionsDF = model.transform(elemsDF)
    val positivePredictionsDF = predictionsDF.filter($"prediction" === 1.0).select(
      $"filename", $"commented".cast("int").alias("line")).as[ModelTransformResult]
    val positivePredictions = positivePredictionsDF.collect()
    val positivePredictionFP = positivePredictions.map(r =>
      FileNameLinePair(r.filename, r.line))
    GetCommentResponse(request.pullRequestURL, positivePredictionFP)
  }
}

case class ModelTransformResult(filename: String, line: Int)

object ModelServingService {
  val issueDataLocation = "gs://frank-the-unicorn/issues-full"
  val pipelineLocation = "gs://frank-the-unicorn/full/ml-ml23a-gbt-withcv-withissues-test/model"
  val onlySpacesRegex = """^(\s+)$""".r
}
