package com.holdenkarau.predict.pr.comments.sparkProject.helper
/**
 * A simple test for switching the encoding to JSON for working with
 */

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.DataFetch
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class JsonDumpTest extends FunSuite with SharedSparkContext {
  val originalInputList = List(
    """pull_request_url,pull_patch_url,comments_positions_space_delimited,comments_original_positions_space_delimited,comment_file_paths_json_encoded,comment_commit_ids_space_delimited""",
    "https://api.github.com/repos/Dreamacro/clash/pulls/96,https://github.com/Dreamacro/clash/pull/96.patch,42 -1,42 42,\"[\"\"\\\"\"rules/from_ipcidr.go\\\"\"\"\",\"\"\\\"\"rules/from_ipcidr.go\\\"\"\"\"]\",\"\"\"de976981dff604f3f41167012ddb82b3e0c90e6d\"\" \"\"0b44c7a83aa400caf5db40975a75428682431309\"\"\"")

  test("e2e test") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(originalInputList, 1)
    val inputPath = s"$tempPath/input.csv"
    val outputPath = s"$tempPath/output.csv"
    val outputPathJson = s"$tempPath/output.json"
    val outputPathJsonEscaped = s"$tempPath/output.json_escaped"
    inputRDD.saveAsTextFile(inputPath)
    val dataFetch = new DataFetch(sc)
    dataFetch.fetch(inputPath, outputPath, None)
    val jsonDump = new JsonDump(sc)
    jsonDump.dump(outputPath, outputPathJson, outputPathJsonEscaped)
    val outputJsonResult = sc.textFile(outputPathJson).collect()(0)
    outputJsonResult should include ("{\"pull_request_url\":")
    val outputJsonResultEscaped = sc.textFile(outputPathJsonEscaped).collect()(0)
    outputJsonResultEscaped should include (
      """{\"pull_request_url\":\"https://api.github.com/repos/Dreamacro/clash/pulls/96\",""")
  }
}
