package com.holdenkarau.predict.pr.comments.sparkProject.helper

import scala.util.matching.Regex

case class PatchRecord(commitId: String, oldPos: Int, newNumber: Int, text: String,
  add: Boolean)
case class DiffRecord(newNumber: Int, text: String)

object PatchExtractor {
  val commitRegex = """^From\s([A-Za-z0-9]+)\s.*$""".r
  val oldFilenameRegex = """^---\sa/(.+)\s?$""".r
  val newFilenameRegex = """^\+\+\+\sb/(.+)\s?$""".r
  // Diff command
  val diffCommandRegex = """^diff --git a/.* b/(.*)$""".r
  // Index
  val indexRegex = """^index\s+(.*)\s*(.*)\s*$""".r
  // Block header
  val blockHeaderRegex = """^@@ -(\d+),\d+ \+(\d+),\d+ @@.*$""".r
  // For now we only use lines that change
  val removedLine = """^-(.*)$""".r
  val addedLine = """^\+(.*)$""".r
  val contextLine = """^\s.*$""".r

  /*
   * Process a given patch. You don't want to look inside this.
   */
  def processPatch(patch: String): Seq[PatchRecord] = {
    val lines = patch.split("\n")
    var commitId: String = null
    var filename: String = null
    var oldPos: Integer = null
    var newPos: Integer = null
    var seenDiff: Boolean = false
    // Loop through the inputs
    lines.flatMap{line =>
      line match {
        case diffCommandRegex(f) =>
          seenDiff = true
          filename = f
          newPos = null
          oldPos = null
          None
        case indexRegex(idx, fileMode) =>
          // Do nothing
          None
        case commitRegex(id) =>
          commitId = id
          // New commit reset all the state
          seenDiff = false
          filename = null
          oldPos = null
          newPos = null
          None
        case oldFilenameRegex(f) if seenDiff && newPos == null =>
          None
        case newFilenameRegex(f) if seenDiff && newPos == null =>
          filename = f
          None
        case blockHeaderRegex(op, np) if seenDiff =>
          oldPos = op.toInt
          newPos = np.toInt
          None
        case addedLine(lineText) if seenDiff && newPos != null =>
          newPos = newPos + 1
          Some(PatchRecord(commitId, oldPos, newPos, lineText, true))
        case removedLine(lineText) if seenDiff && newPos != null =>
          oldPos = oldPos + 1
          Some(PatchRecord(commitId, oldPos, newPos, lineText, false))
        case _ if seenDiff && newPos != null =>
          // Context line
          newPos = newPos + 1
          oldPos = oldPos + 1
          None
        case _ =>
          // Not in diff/patch view
          None
      }
    }
  }
}

