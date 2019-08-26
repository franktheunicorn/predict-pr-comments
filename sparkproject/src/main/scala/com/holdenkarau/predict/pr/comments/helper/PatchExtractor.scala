package com.holdenkarau.predict.pr.comments.sparkProject.helper

import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.PatchRecord
import scala.collection.mutable.Queue
import scala.util.matching.Regex


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
  val contextLine = """^\s(.*)$""".r

  /*
   * Process a given patch. You don't want to look inside this.
   */
  def processPatch(patch: String, diff: Boolean = false, contextLines: Int = 4, nextContextLines: Int =4): Seq[PatchRecord] = {
    val lines = patch.split("\n").toArray
    var commitId: String = null
    var filename: String = null
    var oldPos: Integer = null
    var newPos: Integer = null
    // lines from header starts at 0 for diff view
    var linesFromHeader: Integer = diff match {
      case true => 0
      case false => null
    }
    var seenDiff: Boolean = false
    var previousQueue: Queue[String] = new Queue[String]()
    // Return if this is not a diff command specific line
    def isRegularLine(line: String) = {
      line match {
        case diffCommandRegex(_) =>
          false
        case indexRegex(_, _) =>
          false
        case commitRegex(_) =>
          false
        case oldFilenameRegex(_) =>
          false
        case blockHeaderRegex(_, _) =>
          false
        case contextLine(_) =>
          true
        case addedLine(_) =>
          true
        case removedLine(_) =>
          true
        case _ =>
          false
      }
    }
    def extractLineText(line: String) = {
      line match {
        case addedLine(lineText) => lineText
        case removedLine(lineText) => lineText
        case contextLine(lineText) => lineText
      }
    }
    // Loop through the inputs
    lines.zipWithIndex.flatMap{case (line, index) =>
      line match {
        case diffCommandRegex(f) =>
          previousQueue.dequeueAll(_ => true)
          seenDiff = true
          filename = f
          newPos = null
          oldPos = null
          // See https://developer.github.com/v3/pulls/comments/#create-a-comment
          linesFromHeader = -1
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
          linesFromHeader = null
          None
        case oldFilenameRegex(f) if seenDiff && newPos == null =>
          None
        case newFilenameRegex(f) if seenDiff && newPos == null =>
          filename = f
          None
        case blockHeaderRegex(op, np) if seenDiff || diff =>
          previousQueue.dequeueAll(_ => true)
          oldPos = op.toInt - 1
          newPos = np.toInt - 1
          // Complicated
          linesFromHeader = linesFromHeader + 1
          None
        case addedLine(lineText) if (seenDiff || diff) && newPos != null =>
          previousQueue.enqueue(lineText)
          if (previousQueue.length > contextLines) {
            previousQueue.dequeue()
          }
          val nextLines = lines.slice(index, index + nextContextLines)
            .filter(isRegularLine).map(extractLineText)

          linesFromHeader = linesFromHeader + 1
          newPos = newPos + 1
          if (lineText.length < 2000) {
            Some(
              PatchRecord(
                commitId,
                oldPos,
                newPos,
                Some(linesFromHeader),
                previousQueue.toArray,
                lineText,
                nextLines,
                filename,
                true))
          } else {
            None
          }
        case removedLine(lineText) if (seenDiff || diff) && newPos != null  =>
          previousQueue.enqueue(lineText)
          if (previousQueue.length > contextLines) {
            previousQueue.dequeue()
          }

          val nextLines = lines.slice(index, index + nextContextLines)
            .filter(isRegularLine).map(extractLineText)

          linesFromHeader = linesFromHeader + 1
          oldPos = oldPos + 1
          if (lineText.length < 2000) {
            Some(
              PatchRecord(
                commitId,
                oldPos,
                newPos,
                Some(linesFromHeader),
                previousQueue.toArray,
                lineText,
                nextLines,
                filename,
                false))
          } else {
            None
          }
        case contextLine(lineText) if (seenDiff || diff) && newPos != null =>
          previousQueue.enqueue(lineText)
          if (previousQueue.length > contextLines) {
            previousQueue.dequeue()
          }

          // Context line
          newPos = newPos + 1
          oldPos = oldPos + 1
          linesFromHeader = linesFromHeader + 1
          None
        case _ =>
          println(s"idk what ${line} was")
          // Not in diff/patch view
          None
      }
    }
  }
}

