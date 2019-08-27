package com.holdenkarau.predict.pr.comments.sparkProject.ml

/**
 * A simple test to make sure that we produce the correct labeled features
 */
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep._

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalactic._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class LabeledFeatureTest extends FunSuite with SharedSparkContext {
  val inputRecord = ResultCommentData(
    "https://api.github.com/repos/Dreamacro/clash/pulls/96",
    "https://github.com/Dreamacro/clash/pull/96.patch",
    ParsedCommentInputData(
      "https://api.github.com/repos/Dreamacro/clash/pulls/96",
      "https://github.com/Dreamacro/clash/pull/96.patch",
      List(
        CommentPosition(Some(11), Some(11))
      ),
      Array("text"),
      Array("@@ -33,15 +33,17 @@ type SourceType int\n type Metadata struct {\n \tNetWork  NetWork\n \tSource   SourceType\n+\tSourceIP *net.IP\n \tAddrType int\n \tHost     string\n \tIP       *net.IP\n \tPort     string\n }\n \n func (addr *Metadata) String() string {"),
      List("constant/metadata.go"),
      List("5bdac33935f10137ae726c44dea6737ec7ab520a")),
    "",
    "")

  implicit val labeledRecordEq =
    new Equality[LabeledRecord] {
      def areEqual(a: LabeledRecord, b: Any): Boolean = {
        b match {
          case c: LabeledRecord =>
            if (a.lineText == c.lineText &&
              a.filename == c.filename &&
              a.add == c.add &&
              a.commented == c.commented &&
              a.line == c.line &&
              a.commit_id == c.commit_id &&
              a.comment_text == c.comment_text &&
              a.offset == c.offset) {
              // Kind of sketchy but gives us nice UI
              try {
                a.previousLines should be (c.previousLines)
                a.nextLines should be (c.nextLines)
                true
              } catch {
                case e: org.scalatest.exceptions.TestFailedException =>
                  false
              }
            } else {
              false
            }
          case _ =>
            false
        }
      }
    }


  test("test extracting from diff hunks") {
    val results = Featurizer.produceRecordsFromDiffHunks(inputRecord.parsed_input)
    val expected = LabeledRecord(
      previousLines=Seq(
        "type Metadata struct {",
 	"	NetWork  NetWork",
 	"	Source   SourceType",
        "	SourceIP *net.IP"),
      lineText="	SourceIP *net.IP",
      nextLines=Seq(
        "	SourceIP *net.IP",
        "	AddrType int",
        "	Host     string",
        "	IP       *net.IP"),
      filename="constant/metadata.go",
      add=true,
      commented=true,
      comment_text="text",
      line=35)
    results.size should be (1)
    results should contain (expected)
  }


  test("test we extract the correct labeled features") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val input = session.createDataset(List(inputRecord))
    println("Input is:")
    input.show()
    val labeledRecords = input.flatMap(Featurizer.produceRecord)
    val localRecords = labeledRecords.collect()
    labeledRecords.filter($"commented" === true).collect() should contain (
      LabeledRecord(
        previousLines=Seq(
          "type Metadata struct {",
 	  "	NetWork  NetWork",
 	  "	Source   SourceType",
          "	SourceIP *net.IP"),
        lineText="	SourceIP *net.IP",
        nextLines=Seq(
          "	SourceIP *net.IP",
          "	AddrType int",
          "	Host     string",
          "	IP       *net.IP"),
        filename="constant/metadata.go",
        true,
        true,
        35,
        comment_text="text"
      ))
  }
}

