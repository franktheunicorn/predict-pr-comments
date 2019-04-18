package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

import org.joda.time.DateTime

case class StoredPatch(pull_request_url: String, patch: String, diff: String)
case class CommentPosition(
  comment_position: Option[Int],
  new_comment_position: Option[Int])
case class CommentInputData(
  pull_patch_url: String,
  created_date: DateTime,
  pull_request_url: String,
  comments_positions: Array[CommentPosition],
  diff_hunks: Array[String],
  comment_commit_ids: Array[String],
  comment_file_paths: Array[String])
case class ParsedCommentInputData(
  pull_request_url: String,
  pull_patch_url: String,
  comments_positions: List[CommentPosition],
  diff_hunks: Array[String],
  comment_file_paths: List[String],
  comment_commit_ids: List[String])

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

trait HasPatchUrl{
  val pull_request_url: String
  val pull_patch_url: String
}
