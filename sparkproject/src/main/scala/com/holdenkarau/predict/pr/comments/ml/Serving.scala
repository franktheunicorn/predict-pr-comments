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
import io.grpc.{Server}
import scala.concurrent.{Future, ExecutionContext}

// Logging
import org.apache.log4j.{Logger => Log4jLogger}
import org.apache.log4j.Level

class ModelServingService extends ModelRequestGrpc.ModelRequest {
  import scala.concurrent.ExecutionContext.Implicits.global
  Log4jLogger.getLogger("org").setLevel(Level.OFF)
  val session = SparkSession.builder().master("local[2]").getOrCreate()
  import session.implicits._
  val issueSchema = ScalaReflection.schemaFor[IssueStackTrace].dataType.asInstanceOf[StructType]
  // We don't strongly type here because of query push down fuzzyness
  val issueData = session.read
    .format("org.apache.spark.sql.parquet") // Long name because assembly
    .schema(issueSchema).load(ModelServingService.issueDataLocation)

  println(s"Loading model from $ModelServingService.pipelineLocation")
  val model = PipelineModel.load(ModelServingService.pipelineLocation)

  override def getComment(request: GetCommentRequest) = {
    System.err.println("pandaerr Request kthnx")
    System.out.println("panda Request kthnx")
    System.out.println("panda Request request")
    try {
      ModelServingService.logger.info(s"panda Received request $request")
      val pullRequestPatchURL = request.pullRequestPatchURL
      ModelServingService.logger.info(s"panda using patchURL $pullRequestPatchURL")
      val patchFuture = PatchFetcher.fetchPatchForPatchUrl(pullRequestPatchURL)
      ModelServingService.logger.info(s"Patch future was $patchFuture")
      val responseFuture = patchFuture.map(patch => predictOnResponse(request, patch))
      ModelServingService.logger.info(s"Response future was $responseFuture")
      responseFuture
    } catch {
      case e: Exception =>
        ModelServingService.logger.warning(s"Ran into an error during thing $e")
        Future(GetCommentResponse(message=s"Frank fell down: $e ${e.toString}"))
    }
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
    ModelServingService.logger.info(s"panda predicting on, $repoName $pullRequestURL $patch")
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
    elemsDF.show()
    val predictionsDF = model.transform(elemsDF)
    predictionsDF.show()
    val positivePredictionsDF = predictionsDF.filter($"prediction" === 1.0).select(
      $"filename", $"offset".alias("line"), $"commit_id", $"probability", $"prediction").distinct()
    val distinctPredictions = positivePredictionsDF.groupBy(
      $"filename", $"line", $"commit_id").agg(
        first("prediction").alias("prediction"),
        first("probability").alias("probability"))
    // Try and limit how much help frank gives people
    val actionableResultsDF = distinctPredictions
      //.sort( expr("""element_at(probability, 1)""").desc)
      .limit(7)
      .select($"filename", $"line", $"commit_id")
      .as[ModelTransformResult]
    actionableResultsDF.show()
    val predictions = actionableResultsDF.collect()
    val predictionFP = predictions.map(r =>
      FileNameCommitIDPosition(
        r.filename,
        r.commit_id.get,
        r.line.get))
    println(s"Asking frank to tell folks about  $predictions")
    GetCommentResponse(pullRequestURL, predictionFP)
  }
}

case class ModelTransformResult(filename: String, line: Option[Int], commit_id: Option[String])


class ModelServingServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  def start(): Unit = {
    val port = 777
    server = io.grpc.netty.NettyServerBuilder.forPort(port)
      .addService(
        ModelRequestGrpc.bindService(new ModelServingService, executionContext))
      .build.start
    ModelServingService.logger.info(s"Server started, listening on $port")
    ModelServingService.logger.info(s"Server is $server")
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
