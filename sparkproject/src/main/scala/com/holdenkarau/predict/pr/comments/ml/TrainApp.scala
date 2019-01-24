package com.holdenkarau.predict.pr.comments.sparkProject.ml

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to train a trivial model
  */
object MlSCApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))
  val conf = new SparkConf()

  MyRunner.run(conf, inputFile, outputFile)
}

private object MyRunner {
  def run(conf: SparkConf, inputFile: String, outputFile: String) = {

    val sc = new SparkContext(conf)
    val trainingPipeline = new TrainingPipeline(sc)
    trainingPipeline.trainAndSaveModel(inputFile, outputFile)
  }
}
