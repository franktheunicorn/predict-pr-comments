package com.holdenkarau.predict.pr.comments.sparkProject.ml

import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.{ExecutionContext, Future}


object ServingApp extends App {
  ModelServingService.logger.info(s"Creating server")
  val modelServingServer = new ModelServingServer(ExecutionContext.global)
  ModelServingService.logger.info(s"Starting server")
  modelServingServer.start()
  ModelServingService.logger.info(s"Waiting for shut down")
  modelServingServer.blockUntilShutdown()
  ModelServingService.logger.info(s"Done")
}
