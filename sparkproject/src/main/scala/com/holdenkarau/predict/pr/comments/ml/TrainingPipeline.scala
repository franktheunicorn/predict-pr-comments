package com.holdenkarau.predict.pr.comments.sparkProject.ml

import org.apache.spark.SparkContext
import org.apache.spark.sql._

import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.ResultData
import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor

case class LabeledRecord(text: String, add: Boolean, commented: Boolean)

class TrainingPipeline(sc: SparkContext) {

}

object TrainingPipeline {
  // Take an indivudal PR and produce a sequence of labeled records
  def produceRecord(input: ResultData): Seq[LabeledRecord] = {
    val patchLines = PatchExtractor.processPatch(input.patch)
    val initialRecords = patchLines.map(patchRecord =>
      LabeledRecord(patchRecord.text, patchRecord.add,
        input.comments_positions.contains(Some(patchRecord.newNumber))))
    //The same line may show up in multiple patches
    initialRecords
  }
}
