package com.holdenkarau.predict.pr.comments.sparkProject.ml

import scala.collection.mutable.HashSet
import scala.collection.immutable.{HashSet => ImmutableHashSet}
import com.github.marklister.collections._

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.holdenkarau.predict.pr.comments.sparkProject.helper.PatchExtractor
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{
  ResultCommentData, PatchRecord, ParsedCommentInputData, IssueStackTrace}


case class LabeledRecord(
  previousLines: Seq[String],
  lineText: String,
  nextLines: Seq[String],
  filename: String,
  add: Boolean,
  commented: Boolean,
  line: Int,
  commit_id: Option[String]=None,
  offset: Option[Int]=None,
  comment_text: String="")
// LabeledRecord - line + extension and label and line length as a double + found in issue
case class PreparedData(
  previousLines: Seq[String],
  lineText: String,
  nextLines: Seq[String],
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


class Featurizer(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  // Produce data for training
  def prepareTrainingData(input: Dataset[ResultCommentData],
    issues: Dataset[IssueStackTrace], pyOnly: Boolean = true): Dataset[PreparedData] = {

    val labeledRecords: Dataset[LabeledRecord] = input.flatMap(Featurizer.produceRecord)
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
    val extractExtensionUDF = udf(Featurizer.extractExtension _)
    val annotated = recordsWithIssues.withColumn(
      "extension", extractExtensionUDF($"labaeledRecords.filename"))
      .withColumn(
        "label", $"commented".cast("double"))
      .withColumn(
        "line_length", length($"lineText").cast("double"))
      .withColumn(
        "only_spaces", expr("""lineText rlike "^\s+$" """).cast("double"))
      .withColumn(
        "not_in_issues", isnull($"pandas").cast("double"))
      .select(
        $"previousLines",
        $"lineText",
        $"nextLines",
        $"labaeledRecords.filename",
        $"add",
        $"commented",
        $"extension",
        $"line_length",
        $"label",
        $"only_spaces",
        $"not_in_issues",
        $"commit_id",
        $"offset")
      .as[PreparedData]
    pyOnly match {
      case true => annotated.filter($"extension" === "py")
      case false => annotated
    }
  }
}

object Featurizer {
  val extensionRegex = """.+[a-zA-Z0-9]+\.([a-zA-Z\+]*?)$""".r

  def extractExtension(filename: String): Option[String] = {
    filename.toLowerCase match {
      case extensionRegex(ext) if FileFormats.formats.contains(ext)  => Some(ext)
      case _ => None
    }
  }

  // First off we produce records using the along-side diff hunks
  def produceRecordsFromDiffHunks(parsed_input: ParsedCommentInputData): Seq[LabeledRecord] = {
    if (parsed_input.diff_hunks != null) {
      println(s"Looking at ${parsed_input}")
      val patchLines = parsed_input.diff_hunks.map{
        diff => PatchExtractor.processPatch(diff, diff=true)}
      println(s"Got lines ${patchLines.toSeq}")
      val patchLinesWithComments = (patchLines.toSeq
        .flatZip(parsed_input.diff_hunks)
        .flatZip(parsed_input.comment_file_paths)
        .flatZip(parsed_input.comment_text))
      println(s"Preparing to extract on ${patchLinesWithComments.toSeq}") 
      patchLinesWithComments.flatMap{
        case (patchRecords, diff_hunk, commentFilename, comment_text) =>
          patchRecords
            .map(patchRecord =>
              LabeledRecord(
                previousLines=patchRecord.previousLines,
                lineText=patchRecord.text,
                nextLines=patchRecord.nextLines,
                filename=commentFilename,
                add=patchRecord.add,
                commented=true,
                line=patchRecord.oldPos,
                comment_text=comment_text))
      }
    } else {
      Seq.empty[LabeledRecord]
    }
  }

  // Now we produce candidate records from the patch
  def produceRecordsFromPatch(input: ResultCommentData): Seq[LabeledRecord] = {
    val parsed_input = input.parsed_input
    // Make hash sets for fast lookup of lines which have been commented on
    val commentsOnCommitIdsWithNewLineWithFile = ImmutableHashSet(
      parsed_input.comment_commit_ids
        .flatZip(parsed_input.comment_positions.map(
          _.new_position))
        .flatZip(parsed_input.comment_file_paths):_*)
    val commentsOnCommitIdsWithOldLineWithFile = ImmutableHashSet(
      parsed_input.comment_commit_ids
        .flatZip(parsed_input.comment_positions.map(
          _.original_position))
        .flatZip(parsed_input.comment_file_paths):_*)
    // More loosely make the same hash set but without the commit ID
    val commentsWithNewLineWithFile = ImmutableHashSet(
      parsed_input.comment_positions.map(
        _.new_position)
        .flatZip(parsed_input.comment_file_paths):_*)
    val commentsWithOldLineWithFile = ImmutableHashSet(
      parsed_input.comment_positions.map(
        _.original_position)
        .flatZip(parsed_input.comment_file_paths):_*)

    def recordHasBeenCommentedOn(patchRecord: PatchRecord) = {
      commentsOnCommitIdsWithNewLineWithFile(
        (patchRecord.commitId, Some(patchRecord.newPos), patchRecord.filename)) ||
      commentsOnCommitIdsWithOldLineWithFile(
        (patchRecord.commitId, Some(patchRecord.oldPos), patchRecord.filename))
    }

    // Filter out negative records which are "too close" to comments
    def recordIsCloseToAComment(patchRecord: PatchRecord) = {
      if (recordHasBeenCommentedOn(patchRecord)) {
        false
      } else {
        val fuzzyLines = List(
          patchRecord.newPos-1,
          patchRecord.newPos+1,
          patchRecord.newPos,
          patchRecord.oldPos,
          patchRecord.oldPos-1,
          patchRecord.oldPos+1).map(Some(_))
        fuzzyLines.exists(line =>
          commentsWithNewLineWithFile(line, patchRecord.filename)
            || commentsWithOldLineWithFile(line, patchRecord.filename))
      }
    }

    val patchLines = PatchExtractor.processPatch(input.patch)
      .filter(record => recordIsCloseToAComment(record))
    patchLines.map(patchRecord =>
      LabeledRecord(
        previousLines=patchRecord.previousLines,
        lineText=patchRecord.text,
        nextLines=patchRecord.nextLines,
        filename=patchRecord.filename,
        add=patchRecord.add,
        commented=true,
        line=patchRecord.oldPos))
  }


  // Take an indivudal PR and produce a sequence of labeled records
  def produceRecord(input: ResultCommentData): Seq[LabeledRecord] = {
    val initialRecordsWithComments = produceRecordsFromDiffHunks(input.parsed_input)
    val recordsFromPatch = produceRecordsFromPatch(input)
    // Now we combine them
    val candidateRecords = (initialRecordsWithComments ++ recordsFromPatch).distinct

    // The same text may show up in multiple places, if it's commented on in any of those
    // we want to tag it as commented. We could do this per file but per PR for now.
    val commentedRecords = candidateRecords.filter(_.commented)
    // Only output PRs which have a comment that we can resolve
    if (commentedRecords.isEmpty) {
      commentedRecords
    } else {
      // Output in priority order of commented records, commented records with text, and
      // non-commented records only outputting at most once for the same line text.
      val seenText = new HashSet[String]
      val uncommentedRecords = candidateRecords.filter(r => !r.commented)
      val commentedRecordsWithText = commentedRecords.filter(_.comment_text != "")
      val resultRecords = (commentedRecordsWithText ++
        commentedRecords ++
        uncommentedRecords).filter{record => seenText.add(record.lineText)}
      resultRecords
    }
  }
}

