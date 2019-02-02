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
import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.ExecutionContext

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
    val patchFuture = DataFetch.fetchPatchForPatchUrl(pullRequestPatchURL)
    patchFuture.map(patch => predictOnResponse(request, patch))
  }

  def predictOnResponse(request: GetCommentRequest, patchResponse: Response[String]):
      GetCommentResponse = {
    // TODO(holden): check the status processResponse
    //if (diffResponse.code == StatusCodes.Ok) {
    //}
    val patch = patchResponse.unsafeBody
    predictOnPatch(request.repoName, request.pullRequestURL, patch)
  }

  def predictOnPatch(repoName: String, pullRequestURL: String, patch: String) = {
    val patchRecords = PatchExtractor.processPatch(patch=patch, diff=false)
    val elems = patchRecords.map{record =>
      val extension = TrainingPipeline.extractExtension(record.filename).getOrElse(null)
      val oldPos = record.oldPos
      val newPos = record.newPos

      val foundIssueCount = issueData
        .filter($"project" === repoName)
        .filter($"filename" === record.filename)
        .filter($"line" === oldPos)
        .count()
      PreparedData(
        text=record.text,
        filename=record.filename,
        add=record.add,
        // TODO: Hack fix this in training pipeline later
        commented=0.0,// commented, stripped anyways -- Going to encode some info I needed oops
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
        }, // Not in issues
        commit_id = Some(record.commitId),
        offset = record.linesFromHeader
      )
    }
    val elemsDF = session.createDataFrame(elems)
    val predictionsDF = model.transform(elemsDF)
    val positivePredictionsDF = predictionsDF.filter($"prediction" === 1.0).select(
      $"filename", $"offset".alias("line"), $"commit_id").as[ModelTransformResult]
    val positivePredictions = positivePredictionsDF.collect()
    val positivePredictionFP = positivePredictions.map(r =>
      FileNameCommitIDPosition(
        r.filename,
        r.commit_id.get,
        r.line.get))
    GetCommentResponse(pullRequestURL, positivePredictionFP)
  }
}

case class ModelTransformResult(filename: String, line: Option[Int], commit_id: Option[String])


class ModelServingServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  def start(): Unit = {
    val port = 777
    server = ServerBuilder.forPort(port)
      .addService(ModelRequestGrpc.bindService(new ModelServingService, executionContext)).build.start
    ModelServingService.logger.info(s"Server started, listening on $port")
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }
}


object ModelServingService {
  val issueDataLocation = "gs://frank-the-unicorn/issues-full"
  val pipelineLocation = "gs://frank-the-unicorn/full/ml-ml23a-gbt-withcv-withissues-test/model"
  val onlySpacesRegex = """^(\s+)$""".r
  val logger = Logger.getLogger(classOf[ModelServingService].getName)
}
