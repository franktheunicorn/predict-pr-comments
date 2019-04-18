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


import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{
  PatchRecord, ParsedCommentInputData, IssueStackTrace}
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor

case class LabeledRecord(
  previousLines: Array[String],
  lineText: String,
  nextLines: Array[String],
  filename: String,
  add: Boolean,
  lineCommented: Boolean,
  line: Int,
  commit_id: Option[String]=None,
  offset: Option[Int]=None)
// LabeledRecord - line + extension and label and line length as a double + found in issue
case class PreparedData(
  previousLines: Array[String],
  lineText: String,
  nextLines: Array[String],
  previousTextTokenized: Array[String],
  lineTextTokenized: Array[String],
  filename: String,
  add: Boolean,
  commented: Double,
  extension: String,
  line_length: Double,
  label: Double,
  only_spaces: Double,
  not_in_issues: Double,
  commit_id: Option[String]=None,
  offset: Option[Int]=None)


class TrainingPipeline(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  def trainAndSaveModel(input: String, issueInput: String, output: String, dataprepPipelineLocation: String) = {
    // TODO: Do this
    val schema = ScalaReflection.schemaFor[ParsedCommentInputData].dataType.asInstanceOf[StructType]
    val issueSchema = ScalaReflection.schemaFor[IssueStackTrace].dataType.asInstanceOf[StructType]

    val inputData = session.read.format("parquet").schema(schema).load(input).as[ParsedCommentInputData]
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


  // Produce data for training
  def prepareTrainingData(input: Dataset[ParsedCommentInputData],
    issues: Dataset[IssueStackTrace]): Dataset[PreparedData] = {

    val labeledRecords: Dataset[LabeledRecord] = input.flatMap(TrainingPipeline.produceRecord)
    val labeledWithEnd = labeledRecords.withColumn(
      "endFileName", regexp_replace(labeledRecords("filename"), ".*/", ""))
      .alias("labaeledRecords")
    val issuesWithEnd = issues.withColumn(
      "endFileName", regexp_replace(issues("filename"), ".*/", "")).
      withColumn("pandas", lit("pandas"))
      .alias("issues")

    // Do an outer join on the file name and Issue stack trace
    val recordsWithIssues = labeledWithEnd.join(issuesWithEnd,
      usingColumns=List("endFileName", "line"), joinType="left_outer")
    // Extract the extension and cast the label
    val extractExtensionUDF = udf(TrainingPipeline.extractExtension _)
    recordsWithIssues.withColumn(
      "extension", extractExtensionUDF($"labaeledRecords.filename"))
      .withColumn(
        "label", $"commented".cast("double"))
      .withColumn(
        "line_length", length($"text").cast("double"))
      .withColumn(
        "only_spaces", expr("""text rlike "^\s+$" """).cast("double"))
      .withColumn(
        "not_in_issues", isnull($"pandas").cast("double"))
      .select($"text", $"labaeledRecords.filename", $"add", $"commented", $"extension", $"line_length", $"label", $"only_spaces", $"not_in_issues", $"commit_id", $"offset")
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
    input.unpersist(blocking=false)
    result
  }

  def trainAndEvalModel(input: Dataset[ParsedCommentInputData],
    issueInput: Dataset[IssueStackTrace],
    dataprepPipelineLocation: String,
    split: List[Double] = List(0.9, 0.1), fast: Boolean = false) = {

    input.cache()
    println("****PANDA***: dataset summary compute")
    val preparedInput = prepareTrainingData(input, issueInput)
    val (datasetSize, positives) = preparedInput.select(
      count("*"), sum(preparedInput("label").cast("long")))
      .as[(Long, Long)]
      .collect.head

    val splits = input.randomSplit(split.toArray, seed=42)
    val train = splits(0)
    val test = splits(1)
    val model = trainModel(train, issueInput, dataprepPipelineLocation, fast)
    println("****PANDA***: testing")
    val testResult = model.transform(prepareTrainingData(test, issueInput))
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

  def trainModel(input: Dataset[ParsedCommentInputData], issueInput: Dataset[IssueStackTrace],
    dataprepPipelineLocation: String,
    fast: Boolean = false) = {
    val preparedTrainingData = prepareTrainingData(input, issueInput)
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
  def produceRecord(input: ParsedCommentInputData): Seq[LabeledRecord] = {
    val patchLines = input.diff_hunks.map{diff => PatchExtractor.processPatch(diff)}
    val patchLinesWithComments = patchLines
      .zip(input.comments_positions.zip(input.comment_file_paths))
    val initialRecords = patchLinesWithComments.map{case (patchRecords, commentFilenames) =>

      // Construct two hashsets so we can look up if it's been commented on
      val oldLineCommentPositions = ImmutableHashSet(
        commentFilenames.flatMap{case (position, filename) =>
          position.comment_position match {
            case None => None
            case Some(p) => (p, filename)
          }})
      val newLineCommentPositions = ImmutableHashSet(
        commentFilenames.flatMap{case (position, filename) =>
          position.new_comment_position match {
            case None => None
            case Some(p) => (p, filename)
          }})

      def recordHasBeenCommentedOn(patchRecord: PatchRecord) = {
        oldLineCommentPositions(
          (patchRecord.newPos, patchRecord.filename)) ||
        newLineCommentPositions(
          (patchRecord.oldPos, patchRecord.filename))
      }


      // Let's filter out lines which are close to comments, but not comments
      def isCommentNearButNotAt(patchRecord: PatchRecord) = {
        (! recordHasBeenCommentedOn(patchRecord) &&
          (
            oldLineCommentPositions(
              (patchRecord.newPos+1, patchRecord.filename)) ||
            oldLineCommentPositions(
              (patchRecord.newPos-1, patchRecord.filename)) ||
            newLineCommentPositions(
              (patchRecord.oldPos+1, patchRecord.filename)) ||
            newLineCommentPositions(
              (patchRecord.oldPos-1, patchRecord.filename))))
      }

      patchRecords
        .filter(patchRecord => !isCommentNearButNotAt(patchRecord))
        .map(patchRecord =>
          LabeledRecord(
            patchRecord.previousLines,
            patchRecord.text,
            patchRecord.nextLines,
            patchRecord.filename, patchRecord.add,
            recordHasBeenCommentedOn(patchRecord),
            patchRecord.oldPos))
    }
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
