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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{ResultData, PatchRecord}
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor

case class LabeledRecord(text: String, filename: String, add: Boolean, commented: Boolean)
// LabeledRecord + extension and label
case class PreparedData(text: String, filename: String, add: Boolean, commented: Boolean,
  extension: String, label: Double)


class TrainingPipeline(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  def trainAndSaveModel(input: String, output: String) = {
    // TODO: Do this
    val schema = ScalaReflection.schemaFor[ResultData].dataType.asInstanceOf[StructType]

    val inputData = session.read.format("parquet").schema(schema).load(input).as[ResultData]
    val (model, effectiveness) = trainAndEvalModel(inputData)
    model.write.overwrite().save(s"$output/model")
    sc.parallelize(List(effectiveness), 1).saveAsTextFile(s"$output/effectiveness")
  }


  // Produce data for training
  def prepareTrainingData(input: Dataset[ResultData]) = {
    val labeledRecords: Dataset[LabeledRecord] = input.flatMap(TrainingPipeline.produceRecord)
    // The records can be annoying-ish to compute
    labeledRecords.cache()
    labeledRecords.count()
    // Extract the extension and cast the label
    val extractExtensionUDF = udf(TrainingPipeline.extractExtension _)
    labeledRecords.withColumn(
      "extension", extractExtensionUDF(labeledRecords("filename")))
      .withColumn(
        "label", labeledRecords("commented").cast("double")).as[PreparedData]
  }

  def balanceClasses(input: Dataset[PreparedData]) = {
    input.cache()
    val (datasetSize, positives) = input.select(
      count("*"), sum(input("label"))).as[(Long, Double)].collect.head
    val balancingRatio = positives match {
      case 0 => throw new Exception("No positive examples found, refusing to balance")
      case _ => datasetSize / positives
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
    split: List[Double] = List(0.9, 0.1)) = {

    input.cache()
    val splits = input.randomSplit(split.toArray, seed=42)
    val train = splits(0)
    val test = splits(1)
    val model = trainModel(train)
    val testResult = model.transform(prepareTrainingData(test))
    val evaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderPR")
      .setLabelCol("label")
    testResult.show()
    val score = evaluator.evaluate(testResult)
    println(s"Model score: $score")
    (model, score)
  }

  def trainModel(input: Dataset[ResultData]) = {
    val preparedTrainingData = prepareTrainingData(input)
    // Balanace the training data
    val balancedTrainingData = balanceClasses(preparedTrainingData)

    val pipeline = new Pipeline()
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
    // Or let's see about tfidf
    val hashingTf = new HashingTF().setInputCol("tokens").setOutputCol("rawTf")
    val idf = new IDF().setInputCol("rawTf").setOutputCol("tfIdf")
    // Create our charlie brown christmasstree esque feature vector
    val featureVec = new VectorAssembler().setInputCols(
      List("wordvecs", "extension_index").toArray).setOutputCol("features")
    // Create our simple random forest
    val classifier = new RandomForestClassifier()
      .setFeaturesCol("features").setLabelCol("label").setMaxBins(200)
    pipeline.setStages(List(
      extensionIndexer,
      tokenizer,
      word2vec,
      hashingTf,
      idf,
      // Todo: use the word2vec embeding to look for "typo" words ala https://medium.com/@thomasdecaux/build-a-spell-checker-with-word2vec-data-with-python-5438a9343afd
      featureVec,
      classifier).toArray)
    // Try and find some reasonable params
    val paramGrid = new ParamGridBuilder()
      .addGrid(tokenizer.minTokenLength, Array(1, 3))
      .addGrid(classifier.numTrees, Array(1, 10, 20, 40))
      .addGrid(featureVec.inputCols, Array(
        Array("wordvecs", "extension_index"), // Word2Vec for feature perp
        Array("tfIdf", "extension_index") // tf-idf for feature prep
      )).build()

    val evaluator = new BinaryClassificationEvaluator()
    // We have a pretty imbalanced class distribution
      .setMetricName("areaUnderPR")
      .setLabelCol("label")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(3)
      .setCollectSubModels(true)
    cv.fit(balancedTrainingData)
  }
}

object TrainingPipeline {
  val extensionRegex = """.+\.(.*?)$""".r
  def extractExtension(filename: String): Option[String] = {
    filename match {
      case extensionRegex(ext) => Some(ext)
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
