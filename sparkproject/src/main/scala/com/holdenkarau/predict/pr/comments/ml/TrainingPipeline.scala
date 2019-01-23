package com.holdenkarau.predict.pr.comments.sparkProject.ml

import scala.collection.mutable.HashSet

import org.apache.spark.SparkContext
import org.apache.spark.sql._

import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.ResultData
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor

case class LabeledRecord(text: String, filename: String, add: Boolean, commented: Boolean)

class TrainingPipeline(sc: SparkContext) {

}

object TrainingPipeline {
  // Take an indivudal PR and produce a sequence of labeled records
  def produceRecord(input: ResultData): Seq[LabeledRecord] = {
    val commentsOnCommitIdsWithLineWithFile = input.comment_commit_ids
      .zip(input.comments_positions)
      .zip(input.comment_file_paths)
    val patchLines = PatchExtractor.processPatch(input.patch)
    val initialRecords = patchLines.map(patchRecord =>
      LabeledRecord(patchRecord.text, patchRecord.filename, patchRecord.add,
        commentsOnCommitIdsWithLineWithFile.contains(
          (patchRecord.commitId, Some(patchRecord.newNumber), patchRecord.filename)
        ))).distinct
    //The same line may show up in multiple patches, if it's commented on in any of them
    // we want to record it as commented
    val commentedRecords = initialRecords.filter(_.commented)
    val seenTextAndFile = new HashSet[(String, String)]
    val uncommentedRecords = initialRecords.filter(r => !r.commented)
    val resultRecords = (commentedRecords ++ uncommentedRecords).filter{
      record => seenTextAndFile.add((record.text, record.filename))}
    resultRecords
  }
}
