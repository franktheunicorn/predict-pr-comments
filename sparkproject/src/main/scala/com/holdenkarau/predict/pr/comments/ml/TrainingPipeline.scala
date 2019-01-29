package com.holdenkarau.predict.pr.comments.sparkProject.ml

import scala.collection.mutable.HashSet
import scala.collection.immutable.{HashSet => ImmutableHashSet}
import scala.util.matching.Regex
import com.github.marklister.collections._

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


import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{ResultData, PatchRecord}
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor

case class LabeledRecord(text: String, filename: String, add: Boolean, commented: Boolean)
// LabeledRecord + extension and label and line length as a double for vector assembler
case class PreparedData(text: String, filename: String, add: Boolean, commented: Boolean,
  extension: String, line_length: Double, label: Double, only_spaces: Double)


class TrainingPipeline(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  def trainAndSaveModel(input: String, output: String, dataprepPipelineLocation: String) = {
    // TODO: Do this
    val schema = ScalaReflection.schemaFor[ResultData].dataType.asInstanceOf[StructType]

    val inputData = session.read.format("parquet").schema(schema).load(input).as[ResultData]
    // Reparition the inputs
    val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt

    val partitionedInputs = inputData.repartition(inputParallelism)
    val (model, effectiveness, datasetSize, positives) = trainAndEvalModel(partitionedInputs, dataprepPipelineLocation)
    model.write.save(s"$output/model")
    val cvStage = model.stages.last.asInstanceOf[CrossValidatorModel]
    val paramMaps = cvStage.getEstimatorParamMaps.map(_.toString).toList
    val avgMetrics = cvStage.avgMetrics.map(_.toString).toList
    val summary =
      s"Train/model effectiveness was $effectiveness and scores ${avgMetrics}" +
      s" for ${paramMaps} with $positives out of $datasetSize"
    sc.parallelize(List(summary), 1).saveAsTextFile(s"$output/effectiveness")
  }


  // Produce data for training
  def prepareTrainingData(input: Dataset[ResultData]): Dataset[PreparedData] = {
    val labeledRecords: Dataset[LabeledRecord] = input.flatMap(TrainingPipeline.produceRecord)
    // Extract the extension and cast the label
    val extractExtensionUDF = udf(TrainingPipeline.extractExtension _)
    labeledRecords.withColumn(
      "extension", extractExtensionUDF(labeledRecords("filename")))
      .withColumn(
        "label", labeledRecords("commented").cast("double"))
      .withColumn(
        "line_length", length(labeledRecords("text")).cast("double"))
      .withColumn(
        "only_spaces", expr("""text rlike "^\s+$" """).cast("double"))
      .as[PreparedData]
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
    input.unpersist()
    result
  }

  def trainAndEvalModel(input: Dataset[ResultData],
    dataprepPipelineLocation: String,
    split: List[Double] = List(0.9, 0.1), fast: Boolean = false) = {

    input.cache()
    val preparedInput = prepareTrainingData(input)
    val (datasetSize, positives) = preparedInput.select(
      count("*"), sum(preparedInput("label").cast("long")))
      .as[(Long, Long)]
      .collect.head

    val splits = input.randomSplit(split.toArray, seed=42)
    val train = splits(0)
    val test = splits(1)
    val model = trainModel(train, dataprepPipelineLocation, fast)
    val testResult = model.transform(prepareTrainingData(test))
    val evaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderPR")
      .setLabelCol("label")
    testResult.show()
    val score = evaluator.evaluate(testResult)
    println(s"Model score: $score")
    (model, score, datasetSize, positives)
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


      // Do our feature prep seperately from CV search because word2vec is expensive
      prepPipeline.setStages(Array(
        extensionIndexer,
        tokenizer,
        word2vec
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

  def trainModel(input: Dataset[ResultData], dataprepPipelineLocation: String,
    fast: Boolean = false) = {
    val preparedTrainingData = prepareTrainingData(input)
    // Balanace the training data
    val balancedTrainingData = balanceClasses(preparedTrainingData)

    val prepModel = trainAndSaveOrLoadDataPrepModel(balancedTrainingData,
      dataprepPipelineLocation)
    val preppedData = prepModel.transform(balancedTrainingData)
    preppedData.cache().count()
    
    // Create our charlie brown christmasstree esque feature vector
    val featureVec = new VectorAssembler()
      .setInputCols(Array(
        "wordvecs",
        "only_spaces",
        "extension_index",
        "line_length"))
      .setOutputCol("features")

    // Create our simple classifier
    val classifier = new RandomForestClassifier()
      .setFeaturesCol("features").setLabelCol("label").setMaxBins(50)

    // Put our Vector assembler and classifier together
    val estimatorPipeline = new Pipeline().setStages(Array(featureVec, classifier))

    // Try and find some reasonable params
    val paramGridBuilder = new ParamGridBuilder()
    if (!fast && false) {
      paramGridBuilder.addGrid(
        classifier.numTrees, Array(1, 10, 20, 50)
      ).addGrid(
        classifier.minInfoGain, Array(0.0, 0.0001))
    }
    val paramGrid = paramGridBuilder.build()

    val evaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderPR")
      .setLabelCol("label")

    val cv = new CrossValidator()
      .setEstimator(estimatorPipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(3)
      .setCollectSubModels(true)
    val cvModels = cv.fit(preppedData)
    val resultPipeline = new Pipeline().setStages(
      Array(prepModel, cvModels))
    // This should just copy the models over
    resultPipeline.fit(balancedTrainingData)
  }
}

object TrainingPipeline {
  val extensionRegex = """.+[a-zA-Z0-9]+\.([a-zA-Z\+]*?)$""".r

  def extractExtension(filename: String): Option[String] = {
    filename.toLowerCase match {
      case extensionRegex(ext) if FileFormats.formats.contains(ext)  => Some(ext)
      case _ => None
    }
  }
  // TODO: tests explicitly and seperately from rest of pipeline
  // Take an indivudal PR and produce a sequence of labeled records
  def produceRecord(input: ResultData): Seq[LabeledRecord] = {
    val commentsOnCommitIdsWithNewLineWithFile = ImmutableHashSet(
      input.comment_commit_ids
        .flatZip(input.comments_positions).flatZip(input.comment_file_paths):_*)
    val commentsOnCommitIdsWithOldLineWithFile = ImmutableHashSet(
      input.comment_commit_ids
        .flatZip(input.comments_original).flatZip(input.comment_file_paths):_*)
    def recordHasBeenCommentedOn(patchRecord: PatchRecord) = {
      commentsOnCommitIdsWithNewLineWithFile(
        (patchRecord.commitId, Some(patchRecord.newPos), patchRecord.filename)) ||
      commentsOnCommitIdsWithOldLineWithFile(
        (patchRecord.commitId, Some(patchRecord.oldPos), patchRecord.filename))
    }
    val patchLines = PatchExtractor.processPatch(input.patch)
    val initialRecords = patchLines.map(patchRecord =>
      LabeledRecord(patchRecord.text, patchRecord.filename, patchRecord.add,
        recordHasBeenCommentedOn(patchRecord))).distinct
    // The same text may show up in multiple places, if it's commented on in any of those
    // we want to tag it as commented. We could do this per file but per PR for now.
    val commentedRecords = initialRecords.filter(_.commented)
    // Only output PRs which have a comment that we can resolve
    if (commentedRecords.isEmpty) {
      commentedRecords
    } else {
      val seenTextAndFile = new HashSet[String]
      val uncommentedRecords = initialRecords.filter(r => !r.commented)
      val resultRecords = (commentedRecords ++ uncommentedRecords).filter{
        record => seenTextAndFile.add(record.text)}
      resultRecords
    }
  }
}
