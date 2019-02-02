package com.holdenkarau.predict.pr.comments.sparkProject.ml

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to train a trivial model
  */
object MlSCApp extends App{
  val (inputFile, outputFile, dataPrepLocation, issueInput) = (args(0), args(1), args(2), args(3))
  val conf = new SparkConf()

  MyRunner.run(conf, inputFile, outputFile, dataPrepLocation, issueInput)
}

private object MyRunner {
  def run(conf: SparkConf, inputFile: String, outputFile: String, dataprepLocation: String, issueInput: String) = {

    val sc = new SparkContext(conf)
    val trainingPipeline = new TrainingPipeline(sc)
    trainingPipeline.trainAndSaveModel(input=inputFile, output=outputFile, dataprepPipelineLocation=dataprepLocation, issueInput=issueInput)
  }
}
