package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

case class StoredPatch(pull_request_url: String, patch: String, diff: String)
case class CommentInputData(
  pull_patch_url: String,
  created_date: Date,
  pull_request_url: String,
  comments_positions: Array[String],
  diff_hunks: Array[String],
  comments_original_positions: Array[String],
  comment_commit_ids: Array[String],
  comment_file_paths: Array[String])
case class ParsedCommentInputData(
  pull_request_url: String,
  pull_patch_url: String,
  comments_positions: List[String],
  comments_original_positions: List[String],
  comment_file_paths: List[String],
  comment_commit_ids: List[String])
case class ResultCommentData(
  pull_request_url: String,
  pull_patch_url: String,
  // Some comments might not resolve inside of the updated or original spaces
  // hence the Option type.
  comments_positions: List[Option[Int]],
  comments_original: List[Option[Int]],
  comment_file_paths: List[String],
  comment_commit_ids: List[String],
  diff_hunks: Array[String])
case class PatchRecord(commitId: String, oldPos: Int, newPos: Int,
  linesFromHeader: Option[Int], text: String, filename: String, add: Boolean)
case class IssueInputRecord(name: String, url: String)
case class IssueStackTrace(project: String, filename: String, line: Int)
