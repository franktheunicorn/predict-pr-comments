package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

/**
 * A simple test for fetching github patches
 */

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class DataFetchTest extends FunSuite with SharedSparkContext {
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
    val inputData = issueDataFetch.loadInput(session.createDataset(inputRDD)).as[IssueInputRecord]
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


  val standardInputList = List("{\r\n    \"pull_request_url\": \"\\\"https://api.github.com/repos/CepCap/qna/pulls/7\\\"\",\r\n    \"pull_patch_url\": \"\\\"https://github.com/CepCap/qna/pull/7.patch\\\"\",\r\n    \"created_date\": \"\\\"2019-04-12T08:09:18Z\\\"\",\r\n    \"diff_hunks\": [\r\n      \"\\\"@@ -0,0 +1,9 @@\\\\n+class LinksController < ApplicationController\\\\n+  expose :link\\\\n+\\\\n+  def destroy\\\\n+    if current_user&.author_of?(link.linkable.find(params[:parent_id]))\\\"\",\r\n      \"\\\"@@ -1,6 +1,17 @@\\\\n = content_tag(:p, \\\\\\\"#{answer.body} #{\\\\\\\"- best answer\\\\\\\" if answer.best?}\\\\\\\",\\\\n   class: \\\\\\\"answer-id-#{answer.id}#{\\\\\\\" best-answer\\\\\\\" if answer.best?}\\\\\\\", data: { answer_id: answer.id } )\\\\n \\\\n+.answer_links\\\\n+  p Links:\\\\n+  p\\\\n+    - answer.links.each do |link|\\\\n+      p class=\\\\\\\"link-id-#{link.id}\\\\\\\"\\\\n+        -if link.to_gist?\\\\n+          = render partial: 'links/gist', locals: { link: link }\\\\n+        -else\\\\n+          = link_to link.name, link.url\\\\n+        = link_to 'Delete link', link_path(id: link.id, parent_id: answer.id), method: :delete, remote: true, class: \\\\\\\"link-id-#{link.id}\\\\\\\"\\\"\",\r\n      \"\\\"@@ -1,16 +1,25 @@\\\\n - if current_user&.author_of?(question)\\\\n-  p= link_to 'Delete question', question, method: :delete\\\\n-  p= link_to 'Edit question', edit_question_path(question)\\\\n+  .row\\\\n+    p= link_to 'Delete question', question, method: :delete, class: 'col-md-3'\\\\n+    p= link_to 'Edit question', edit_question_path(question), class: 'col-md-3'\\\\n \\\\n h1= question.title\\\\n h5= question.body\\\\n+p= 'Award available!' if question.award.present?\\\\n br\\\\n+.question_links\\\\n+  -question.links.each do |link|\\\\n+    = content_tag(:p, '', class: \\\\\\\"link-id-#{link.id}\\\\\\\")\\\\n+      -if link.to_gist?\\\\n+        = render partial: 'links/gist', locals: { link: link }\\\\n+      -else\\\\n+        = link_to link.name, link.url\\\\n+      = link_to 'Delete link', link_path(id: link.id, parent_id: question.id), method: :delete, remote: true, class: \\\\\\\"link-id-#{link.id}\\\\\\\"\\\"\"\r\n    ],\r\n    \"comment_positions\": [\r\n      {\r\n        \"original_position\": \"5\",\r\n        \"new_position\": \"5\"\r\n      },\r\n      {\r\n        \"original_position\": \"13\",\r\n        \"new_position\": \"13\"\r\n      },\r\n      {\r\n        \"original_position\": \"20\",\r\n        \"new_position\": \"19\"\r\n      }\r\n    ],\r\n    \"comment_commit_ids\": [\r\n      \"\\\"d2b11db69b1abb5cbe0b41c4c17352504d77c25a\\\"\",\r\n      \"\\\"d2b11db69b1abb5cbe0b41c4c17352504d77c25a\\\"\",\r\n      \"\\\"d2b11db69b1abb5cbe0b41c4c17352504d77c25a\\\"\"\r\n    ],\r\n    \"comment_file_paths\": [\r\n      \"\\\"app/controllers/links_controller.rb\\\"\",\r\n      \"\\\"app/views/answers/_answer.html.slim\\\"\",\r\n      \"\\\"app/views/questions/show.html.slim\\\"\"\r\n    ]\r\n  }")


  test("Cleaning input should work") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList)
    val dataFetch = new DataFetch(sc)
    val inputData = dataFetch.loadJsonInput(session.createDataset(inputRDD)).as[CommentInputData]
    inputData.count() should equal (1)
    inputData.collect()(0).comment_positions.size should equal(inputData.collect()(0).diff_hunks.size)
    val cleanedInputData = dataFetch.cleanInputs(inputData).collect()(0)
    cleanedInputData.comment_positions.size should equal(cleanedInputData.diff_hunks.size)

    cleanedInputData.comment_positions should contain theSameElementsAs List(
      CommentPosition(Some(5), Some(5)),
      CommentPosition(Some(13), Some(13)),
      CommentPosition(Some(20), Some(19)))

    cleanedInputData.comment_file_paths should contain theSameElementsAs List(
        "app/controllers/links_controller.rb",
        "app/views/answers/_answer.html.slim",
        "app/views/questions/show.html.slim")
    cleanedInputData.comment_commit_ids should contain theSameElementsAs List(
      "d2b11db69b1abb5cbe0b41c4c17352504d77c25a",
      "d2b11db69b1abb5cbe0b41c4c17352504d77c25a",
      "d2b11db69b1abb5cbe0b41c4c17352504d77c25a")

    // TODO: Add something about the diff hunks in here
  }

  test("calling with a local file fetches a result") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList)
    val dataFetch = new DataFetch(sc)
    val patchFetcher = new PatchFetcher(sc)
    val inputData = dataFetch.loadJsonInput(
      session.createDataset(inputRDD)).as[CommentInputData]
    val cleanedInputData = dataFetch.cleanInputs(inputData)
    val cachedData = session.emptyDataset[StoredPatch]
    val result = patchFetcher.fetchPatches(cleanedInputData, cachedData)
    result.count() should be (1)
  }


  test("cache records are filtered out") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList)
    val dataFetch = new DataFetch(sc)
    val patchFetcher = new PatchFetcher(sc)
    val inputData = dataFetch.loadJsonInput(
      session.createDataset(inputRDD)).as[CommentInputData]
    val basicCached = StoredPatch(
      "https://api.github.com/repos/CepCap/qna/pulls/7",
      "notreal",
      "stillnotreal")
    val cachedData = session.createDataset(sc.parallelize(List(basicCached)))
    val cleanedInputData = dataFetch.cleanInputs(inputData)
    val result = patchFetcher.fetchPatches(cleanedInputData, cachedData)
    result.count() should be (0)
  }


  test("test the main entry point - no cache") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val dataFetch = new DataFetch(sc)

    // Construct the input
    val inputRDD = sc.parallelize(standardInputList, 1)
    val inputData = dataFetch.loadJsonInput(
      session.createDataset(inputRDD)).as[CommentInputData]
    val inputPath = s"$tempPath/input.parquet"
    inputData.write.format("parquet").save(inputPath)

    // Run the test
    val outputPath = s"$tempPath/output.parquet"
    dataFetch.fetch(inputPath, outputPath, None)
    val result = session.read.format("parquet").load(outputPath)
    result.count() should be (1)
  }


  test("test the main entry point - with cache") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val dataFetch = new DataFetch(sc)

    // Construct the input
    val inputRDD = sc.parallelize(standardInputList, 1)
    val inputData = dataFetch.loadJsonInput(
      session.createDataset(inputRDD)).as[CommentInputData]
    val inputPath = s"$tempPath/input.parquet"
    inputData.write.format("parquet").save(inputPath)

    // Run the test
    val outputPath = s"$tempPath/output.parquet"
    val cachePath = s"$tempPath/cache"
    // First run populate the cache
    dataFetch.fetch(inputPath, outputPath, Some(cachePath))
    // Run again, with the cache
    dataFetch.fetch(inputPath, outputPath, Some(cachePath))
    val result = session.read.format("parquet").load(outputPath)
    result.count() should be (1)
    val typedResultHead = result.as[ResultCommentData].collect()(0)
    typedResultHead.patch should include ("Subject: [PATCH")
    typedResultHead.diff should include ("@@ -")
    typedResultHead.diff should not include ("Subject: [PATCH")
    typedResultHead.parsed_input.diff_hunks(0) should include ("@@ -")
    // Json escaping
    typedResultHead.parsed_input.diff_hunks(0) should not include ("\"@@ -")
  }
}
