package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

/**
 * A simple test for fetching github patches
 */

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class IssueFetchTest extends FunSuite with SharedSparkContext {
    test("Issue extraction regex - go") {
    val goStackStrace = """
01 goroutine 1 [running]:
02 main.Example(0x2080c3f50, 0x2, 0x4, 0x425c0, 0x5, 0xa)
           /Users/bill/Spaces/Go/Projects/src/github.com/goinaction/code/
           temp/main.go:9 +0x64
03 main.main()
           /Users/bill/Spaces/Go/Projects/src/github.com/goinaction/code/
           temp/main.go:5 +0x85
"""
      val expected = List(
        IssueStackTrace("a", "main.go", 9),
        IssueStackTrace("a", "main.go", 5))
      val iir = IssueInputRecord("a", "c")
      IssueDataFetch.extractStackTraces((iir, goStackStrace)).toList should contain theSameElementsAs expected
    }

    test("Issue extraction regex - java") {
    val javaStackStrace = """
Exception in thread "main" java.lang.NullPointerException
        at com.example.myproject.Book.getTitle(Book.java:16)
        at com.example.myproject.Author.getBookTitles(Author.java:25)
        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)"""
    val expected = List(
      IssueStackTrace("b", "Book.java", 16),
      IssueStackTrace("b", "Author.java", 25),
      IssueStackTrace("b", "Bootstrap.java", 14))
    val iir = IssueInputRecord("b", "c")
    IssueDataFetch.extractStackTraces((iir, javaStackStrace)).toList should contain theSameElementsAs expected

  }

  test("Live Issue extraction") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val issueDataFetch = new IssueDataFetch(sc)

    val issueInputList = List("name,url",
      "holdenk/spark-testing-base,\"\"\"https://github.com/holdenk/spark-testing-base/issues/132\"\"\"")
    val inputRDD = sc.parallelize(issueInputList, 1)
    val inputData = issueDataFetch.loadInput(
      session.createDataset(inputRDD)).as[IssueInputRecord]
    val result = issueDataFetch.processInput(inputData).collect()

    val expected = List(
      IssueStackTrace("holdenk/spark-testing-base", "TestSuite.scala",13),
      IssueStackTrace("holdenk/spark-testing-base", "FooSpec.scala",8),
      IssueStackTrace("holdenk/spark-testing-base", "DataFrameSuiteBase.scala",83),
      IssueStackTrace("holdenk/spark-testing-base", "FooSpec.scala",8),
      IssueStackTrace("holdenk/spark-testing-base", "FooSpec.scala",14),
      IssueStackTrace("holdenk/spark-testing-base", "FooSpec.scala",9),
      IssueStackTrace("holdenk/spark-testing-base", "OutcomeOf.scala",85))
    result.toList should contain theSameElementsAs expected

  }
}
