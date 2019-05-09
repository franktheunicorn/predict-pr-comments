package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

import java.sql.Date

trait HasPatchUrl{
  val pull_request_url: String
  val pull_patch_url: String
}


case class StoredPatch(pull_request_url: String, patch: String, diff: String)
case class CommentPosition(
  original_position: Option[Int],
  new_position: Option[Int])

case class CommentInputData(
  pull_patch_url: String,
  created_date: String,//ugh
  pull_request_url: String,
  // The JSON dumps from BQ have the comment pos as strings even when cast to ints
  comment_positions: Array[Map[String, String]],
  comment_text: Array[String],
  diff_hunks: Array[String],
  comment_commit_ids: Array[String],
  comment_file_paths: Array[String])
case class ParsedCommentInputData(
  pull_request_url: String,
  pull_patch_url: String,
  comment_positions: List[CommentPosition],
  comment_text: Array[String],
  diff_hunks: Array[String],
  comment_file_paths: List[String],
  comment_commit_ids: List[String]) extends HasPatchUrl

case class ResultCommentData(
  pull_request_url: String,
  pull_patch_url: String,
  parsed_input: ParsedCommentInputData,
  patch: String,
  diff: String) extends HasPatchUrl

case class CommentPatchRecord(
  commitId: String,
  oldPos: Int,
  newPos: Int,
  linesFromHeader: Option[Int],
  lineWithContext: String,
  lineText: String,
  filename: String,
  add: Boolean)

case class IssueInputRecord(
  name: String,
  url: String)

case class IssueStackTrace(project: String,
  filename: String,
  line: Int)

case class PatchRecord(
  commitId: String,
  oldPos: Int,
  newPos: Int,
  linesFromHeader: Option[Int],
  previousLines: Array[String],
  text: String,
  nextLines: Array[String],
  filename: String,
  add: Boolean)

