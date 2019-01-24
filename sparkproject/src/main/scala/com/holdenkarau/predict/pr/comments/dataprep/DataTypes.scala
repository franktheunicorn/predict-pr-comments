package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

case class StoredPatch(pull_request_url: String, patch: String, diff: String)
case class InputData(
  pull_request_url: String,
  pull_patch_url: String,
  comments_positions_space_delimited: String,
  comments_original_positions_space_delimited: String,
  comment_file_paths_json_encoded: String,
  comment_commit_ids_space_delimited: String)
case class ParsedInputData(
  pull_request_url: String,
  pull_patch_url: String,
  comments_positions: List[String],
  comments_original_positions: List[String],
  comment_file_paths: List[String],
  comment_commit_ids: List[String])
case class ResultData(
  pull_request_url: String,
  pull_patch_url: String,
  // Some comments might not resolve inside of the updated or original spaces
  // hence the Option type.
  comments_positions: List[Option[Int]],
  comments_original: List[Option[Int]],
  comment_file_paths: List[String],
  comment_commit_ids: List[String],
  patch: String,
  diff: String)
case class PatchRecord(commitId: String, oldPos: Int, newPos: Int, text: String,
  filename: String, add: Boolean)
