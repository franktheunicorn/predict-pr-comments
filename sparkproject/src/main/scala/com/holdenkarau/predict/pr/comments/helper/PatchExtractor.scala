package com.holdenkarau.predict.pr.comments.sparkProject.helper

import scala.util.matching.Regex

case class PatchRecord(commitId: String, oldPos: Int, newNumber: Int, text: String,
  add: Boolean)
case class DiffRecord(newNumber: Int, text: String)

object PatchExtractor {
  val commitRegex = raw"^From\s(\w+)\s".r
  val fileNameRegex = """^%+++\sb/(\w+)\s?$""".r
  // Block header
  val indexRegex = """^@@ -(\d+),\d+ \+(\d+),\d+ @@.*$""".r
  // For now we only use lines that change
  val removedLine = """^\+(.*)$""".r
  val addedLine = """^-(.*)$""".r

  def processPatch(patch: String): Seq[PatchRecord] = {
    val lines = patch.split("\n")
    var commitId: String = null
    var filename: String = null
    var oldPos: Integer = null
    var newPos: Integer = null
    // Loop through the inputs
    lines.flatMap{
      _ match {
        case commitRegex(id) =>
          commitId = id
          filename = null
          None
        case fileNameRegex(f) =>
          filename = f
          None
        case indexRegex(op, np) =>
          oldPos = op.toInt
          newPos = np.toInt
          None
        case addedLine(lineText) =>
          newPos = newPos + 1
          Some(PatchRecord(commitId, oldPos, newPos, lineText, true))
        case removedLine(lineText) =>
          oldPos = oldPos + 1
          Some(PatchRecord(commitId, oldPos, newPos, lineText, false))
        case _ =>
          newPos = newPos + 1
          oldPos = oldPos + 1
          None
      }
    }
  }
}

