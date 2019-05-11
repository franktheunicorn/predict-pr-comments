package com.holdenkarau.predict.pr.comments.sparkProject.ml

import scala.util.matching.Regex

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{
  ResultCommentData, PatchRecord, ParsedCommentInputData, IssueStackTrace}



class TrainingPipeline(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  val featurizer = new Featurizer(sc)

  def trainAndSaveModel(input: String, issueInput: String, output: String, dataprepPipelineLocation: String) = {
    // TODO: Do this
    val schema = ScalaReflection.schemaFor[ResultCommentData].dataType.asInstanceOf[StructType]
    val issueSchema = ScalaReflection.schemaFor[IssueStackTrace].dataType.asInstanceOf[StructType]

    val inputData = session.read.format("parquet").schema(schema).load(input).as[ResultCommentData]
    val issueInputData = session.read.format("parquet").schema(issueSchema).load(issueInput).as[IssueStackTrace]
    // Reparition the inputs
    val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt

    val partitionedInputs = inputData.repartition(inputParallelism)
    val (model, prScore, rocScore, datasetSize, positives) = trainAndEvalModel(partitionedInputs, issueInputData, dataprepPipelineLocation)
    model.write.save(s"$output/model")
    val cvSummary = model.stages.last match {
      case cvStage: CrossValidatorModel =>
        val paramMaps = cvStage.getEstimatorParamMaps.map(_.toString).toList
        val avgMetrics = cvStage.avgMetrics.map(_.toString).toList
        s"CV pr scores (on balanced classes!): ${avgMetrics} for ${paramMaps}"
      case _ =>
        "The final stage was not CV so no CV summary"
    }
    val dataSummary = s"with $positives out of $datasetSize"
    val summary = s"Train/model effectiveness (no-rebalance) was pr: $prScore roc: $rocScore $cvSummary & data: $dataSummary"
    sc.parallelize(List(summary), 1).saveAsTextFile(s"$output/effectiveness")
  }

  def balanceClasses(input: Dataset[PreparedData]): Dataset[PreparedData] = {
    input.cache()
    // Double so we can get fractional results
    val (datasetSize, positives) = input.select(
      count("*"), sum(input("label"))).as[(Long, Double)].collect.head
    val balancingRatio = positives match {
      case 0.0 => throw new Exception("No positive examples found, refusing to balance")
      case _ => (datasetSize / positives).ceil
    }
    // Some of the algs support weightCol some don't so just duplicate records
    val result = input.flatMap{record =>
      if (record.label != 0.0) {
        List.fill(balancingRatio.toInt)(record)
      } else {
        Some(record)
      }
    }
    result.cache()
    result.count()
    input.unpersist(blocking=false)
    result
  }

  def trainAndEvalModel(input: Dataset[ResultCommentData],
    issueInput: Dataset[IssueStackTrace],
    dataprepPipelineLocation: String,
    split: List[Double] = List(0.9, 0.1), fast: Boolean = false) = {

    input.cache()
    println("****PANDA***: dataset summary compute")
    val preparedInput = featurizer.prepareTrainingData(input, issueInput)
    val (datasetSize, positives) = preparedInput.select(
      count("*"), sum(preparedInput("label").cast("long")))
      .as[(Long, Long)]
      .collect.head

    val splits = input.randomSplit(split.toArray, seed=42)
    val train = splits(0)
    val test = splits(1)
    val model = trainModel(train, issueInput, dataprepPipelineLocation, fast)
    println("****PANDA***: testing")
    val testResult = model.transform(featurizer.prepareTrainingData(test, issueInput))
    // We don't need as much data anymore :)
    train.unpersist(blocking = false)
    testResult.cache()
    testResult.count()
    println("****PANDA***: test result set computed")
    input.unpersist(blocking = false)
    test.unpersist(blocking = false)
    // Make both PR and ROC evaluators
    println("****PANDA***: evaluating")
    val prEvaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderPR")
      .setLabelCol("label")
    val prScore = prEvaluator.evaluate(testResult)
    val rocEvaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderROC")
      .setLabelCol("label")
    testResult.show()
    val rocScore = rocEvaluator.evaluate(testResult)
    println(s"Model score: P:$prScore R:$rocScore")
    (model, prScore, rocScore, datasetSize, positives)
  }

  // This is kind of a hack because word2vec is expensive and we want to expirement
  // downstream less expensively.
  def trainAndSaveOrLoadDataPrepModel(input: Dataset[PreparedData],
    dataprepPipelineLocation: String): PipelineModel = {
    // Try and load the model
    def loadModel(path: String): PipelineModel = {
      PipelineModel.load(path)
    }
    // Train a new model and save it if we don't have to go from
    def trainAndSaveModel(path: String): PipelineModel = {
      val prepPipeline = new Pipeline()
      // Turn our different file names into string indexes
      val extensionIndexer = new StringIndexer()
        .setHandleInvalid("keep") // Some files no extensions
        .setInputCol("extension")
        .setOutputCol("extension_index")
      // For now we use the default tokenizer
      // In the future we could be smart based on programming language
      val tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("tokens")
      // See the sorced tech post about id2vech - https://blog.sourced.tech/post/id2vec/
      val word2vec = new Word2Vec().setInputCol("tokens").setOutputCol("wordvecs")
      //val hashingTf = new HashingTF().setInputCol("tokens").setOutputCol("rawTf")
      //val idf = new IDF().setInputCol("rawTf").setOutputCol("tf_idf").setMinDocFreq(10)
      // Create our charlie brown christmasstree esque feature vector
      val featureVec = new VectorAssembler()
        .setInputCols(Array(
          "wordvecs",
          "only_spaces",
          "extension_index",
          //"tf_idf",
          "line_length",
          "not_in_issues"))
        .setOutputCol("features")



      // Do our feature prep seperately from CV search because word2vec is expensive
      prepPipeline.setStages(Array(
        extensionIndexer,
        tokenizer,
        word2vec,
        //hashingTf,
        //idf,
        featureVec
        ))
      val prepModel = prepPipeline.fit(input)
      prepModel.write.overwrite().save(dataprepPipelineLocation)
      prepModel
    }
    val model = try {
      loadModel(dataprepPipelineLocation)
    } catch {
      case _: Exception => trainAndSaveModel(dataprepPipelineLocation)
    }
    model
  }

  def trainModel(input: Dataset[ResultCommentData], issueInput: Dataset[IssueStackTrace],
    dataprepPipelineLocation: String,
    fast: Boolean = false) = {
    val preparedTrainingData = featurizer.prepareTrainingData(input, issueInput)
    // Balanace the training data
    println("****PANDA***: balancing classes.")
    val balancedTrainingData = balanceClasses(preparedTrainingData)

    println("****PANDA***: training or loading data prep pipeline")
    val prepModel = trainAndSaveOrLoadDataPrepModel(balancedTrainingData,
      dataprepPipelineLocation)
    println("****PANDA***: prepairing data")
    val preppedData = prepModel.transform(balancedTrainingData)
    preppedData.cache().count()
    println("****PANDA***: prepaired data")


    // Create our simple classifier
    val classifier = new GBTClassifier()
      .setFeaturesCol("features").setLabelCol("label").setMaxBins(50)

    // Try and find some reasonable params
    val paramGridBuilder = new ParamGridBuilder()
    if (!fast) {
      paramGridBuilder.addGrid(
        classifier.minInfoGain, Array(0.0, 0.0001)
      )
    }
    val paramGrid = paramGridBuilder.build()

    val evaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderPR")
      .setLabelCol("label")

    val cv = new CrossValidator()
      .setEstimator(classifier)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2)
      .setCollectSubModels(false)
    val fitModel = cv.fit(preppedData)
    println("****PANDA***: fitting model")
    /*
    val fitModel = classifier.fit(preppedData)
     */
    println("****PANDA***: fitting final pipeline")
    val resultPipeline = new Pipeline().setStages(
      Array(prepModel, fitModel))
    // This should just copy the models over
    val rpm = resultPipeline.fit(balancedTrainingData)
    println("****PANDA***: done fitting model")
    rpm
  }
}
