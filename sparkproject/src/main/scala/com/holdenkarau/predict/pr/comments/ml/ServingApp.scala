package com.holdenkarau.predict.pr.comments.sparkProject.ml

import org.apache.spark.{SparkConf, SparkContext}
import scala.concurrent.{ExecutionContext, Future}


object ServingApp extends App {
  val modelServingServer = new ModelServingServer(ExecutionContext.global)
  modelServingServer.start()
  modelServingServer.blockUntilShutdown()
}
