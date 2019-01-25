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
// LabeledRecord + extension and label and line length as a double for vector assembler
case class PreparedData(text: String, filename: String, add: Boolean, commented: Boolean,
  extension: String, lineLength: Double, label: Double)


class TrainingPipeline(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  def trainAndSaveModel(input: String, output: String) = {
    // TODO: Do this
    val schema = ScalaReflection.schemaFor[ResultData].dataType.asInstanceOf[StructType]

    val inputData = session.read.format("parquet").schema(schema).load(input).as[ResultData]
    // Reparition the inputs
    val inputParallelism = sc.getConf.get("spark.default.parallelism", "100").toInt

    val partitionedInputs = inputData.repartition(inputParallelism)
    val (model, effectiveness, datasetSize, positives) = trainAndEvalModel(partitionedInputs)
    model.write.overwrite().save(s"$output/model")
    val summary =
      s"Train/model effectiveness was $effectiveness and scores ${model.avgMetrics}" +
      s" for ${model.estimatorParamMaps} with $positives out of $datasetSize"
    sc.parallelize(List(effectiveness), 1).saveAsTextFile(s"$output/effectiveness")
  }


  // Produce data for training
  def prepareTrainingData(input: Dataset[ResultData]) = {
    val labeledRecords: Dataset[LabeledRecord] = input.flatMap(TrainingPipeline.produceRecord)
    // Extract the extension and cast the label
    val extractExtensionUDF = udf(TrainingPipeline.extractExtension _)
    labeledRecords.withColumn(
      "extension", extractExtensionUDF(labeledRecords("filename")))
      .withColumn(
        "label", labeledRecords("commented").cast("double"))
      .withColumn(
        "line_length", length(labeledRecords("text")).cast("double"))
      .as[PreparedData]
  }

  def balanceClasses(input: Dataset[PreparedData]) = {
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
    val model = trainModel(train, fast)
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

  def trainModel(input: Dataset[ResultData], fast: Boolean=false) = {
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
      List("wordvecs", "extension_index", "line_length").toArray).setOutputCol("features")
    // Create our simple random forest
    val classifier = new RandomForestClassifier()
      .setFeaturesCol("features").setLabelCol("label").setMaxBins(50)
    pipeline.setStages(List(
      extensionIndexer,
      tokenizer,
      word2vec,
      //hashingTf,
      //idf,
      // Todo: use the word2vec embeding to look for "typo" words ala https://medium.com/@thomasdecaux/build-a-spell-checker-with-word2vec-data-with-python-5438a9343afd
      featureVec,
      classifier).toArray)
    // Try and find some reasonable params
    val paramGridBuilder = new ParamGridBuilder()
    if (!fast) {
      paramGridBuilder.addGrid(classifier.numTrees, Array(1, 10))
        /*.addGrid(featureVec.inputCols, Array(
          Array("wordvecs", "extension_index"), // Word2Vec for feature perp
          Array("tfIdf", "extension_index") // tf-idf for feature prep
        ))*/
    }
    val paramGrid = paramGridBuilder.build()

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
  val extensionRegex = """.+[a-zA-Z0-9]+\.([a-zA-Z\+]*?)$""".r

  def extractExtension(filename: String): Option[String] = {
    filename.toLowerCase match {
      case extensionRegex(ext) if ext.length < 6 &&  => Some(ext)
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
