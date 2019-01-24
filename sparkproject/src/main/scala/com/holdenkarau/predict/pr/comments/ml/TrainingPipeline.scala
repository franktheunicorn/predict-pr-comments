package com.holdenkarau.predict.pr.comments.sparkProject.ml

import scala.collection.mutable.HashSet
import scala.collection.immutable.{HashSet => ImmutableHashSet}
import scala.util.matching.Regex
import com.github.marklister.collections._

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Word2Vec, Tokenizer, StringIndexer, SQLTransformer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{ResultData, PatchRecord}
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor

case class LabeledRecord(text: String, filename: String, add: Boolean, commented: Boolean)

class TrainingPipeline(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  def trainAndSaveModel(input: String, output: String) = {
    // TODO: Do this
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
        "label", labeledRecords("commented").cast("double"))
  }

  def trainModel(input: Dataset[ResultData]) = {
    val preparedTrainingData = prepareTrainingData(input)
    val pipeline = new Pipeline()
    // Turn our different file names into string indexes
    val extensionIndexer = new StringIndexer()
      .setHandleInvalid("keep") // Some files no extensions
      .setInputCol("extension")
      .setOutputCol("extension_index")
    // For now we use the default tokenizer
    // In the future we could be smart based on programming language
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
    // See the sorced tech post about id2vech - https://blog.sourced.tech/post/id2vec/
    val word2vec = new Word2Vec().setInputCol("tokens").setOutputCol("wordvecs")
    // Create our charlie brown christmasstree esque feature vector
    val featureVec = new VectorAssembler().setInputCols(
      List("wordvecs", "extension_index").toArray).setOutputCol("features")
    // Create our simple random forest
    val forest = new RandomForestClassifier()
      .setFeaturesCol("features").setLabelCol("label")
    pipeline.setStages(List(
      extensionIndexer,
      tokenizer,
      word2vec,
      featureVec,
      forest).toArray)
    pipeline.fit(preparedTrainingData)
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
    //The same line may show up in multiple patches, if it's commented on in any of them
    // we want to record it as commented
    val commentedRecords = initialRecords.filter(_.commented)
    // Only output PRs which have a comment that we can resolve
    if (commentedRecords.isEmpty) {
      commentedRecords
    } else {
      val seenTextAndFile = new HashSet[(String, String)]
      val uncommentedRecords = initialRecords.filter(r => !r.commented)
      val resultRecords = (commentedRecords ++ uncommentedRecords).filter{
        record => seenTextAndFile.add((record.text, record.filename))}
      resultRecords
    }
  }
}
