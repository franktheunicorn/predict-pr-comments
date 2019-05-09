package com.holdenkarau.predict.pr.comments.sparkProject.helper
/**
 * A simple test for switching the encoding to JSON for working with
 */

import com.holdenkarau.spark.testing.{SharedSparkContext, Utils}
import com.holdenkarau.predict.pr.comments.sparkProject.dataprep.{DataFetch, CommentInputData}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class JsonDumpTest extends FunSuite with SharedSparkContext {
    val standardInputList = List("{\r\n    \"pull_request_url\": \"\\\"https://api.github.com/repos/CepCap/qna/pulls/7\\\"\",\r\n    \"pull_patch_url\": \"\\\"https://github.com/CepCap/qna/pull/7.patch\\\"\",\r\n    \"created_date\": \"\\\"2019-04-12T08:09:18Z\\\"\",\r\n    \"diff_hunks\": [\r\n      \"\\\"@@ -0,0 +1,9 @@\\\\n+class LinksController < ApplicationController\\\\n+  expose :link\\\\n+\\\\n+  def destroy\\\\n+    if current_user&.author_of?(link.linkable.find(params[:parent_id]))\\\"\",\r\n      \"\\\"@@ -1,6 +1,17 @@\\\\n = content_tag(:p, \\\\\\\"#{answer.body} #{\\\\\\\"- best answer\\\\\\\" if answer.best?}\\\\\\\",\\\\n   class: \\\\\\\"answer-id-#{answer.id}#{\\\\\\\" best-answer\\\\\\\" if answer.best?}\\\\\\\", data: { answer_id: answer.id } )\\\\n \\\\n+.answer_links\\\\n+  p Links:\\\\n+  p\\\\n+    - answer.links.each do |link|\\\\n+      p class=\\\\\\\"link-id-#{link.id}\\\\\\\"\\\\n+        -if link.to_gist?\\\\n+          = render partial: 'links/gist', locals: { link: link }\\\\n+        -else\\\\n+          = link_to link.name, link.url\\\\n+        = link_to 'Delete link', link_path(id: link.id, parent_id: answer.id), method: :delete, remote: true, class: \\\\\\\"link-id-#{link.id}\\\\\\\"\\\"\",\r\n      \"\\\"@@ -1,16 +1,25 @@\\\\n - if current_user&.author_of?(question)\\\\n-  p= link_to 'Delete question', question, method: :delete\\\\n-  p= link_to 'Edit question', edit_question_path(question)\\\\n+  .row\\\\n+    p= link_to 'Delete question', question, method: :delete, class: 'col-md-3'\\\\n+    p= link_to 'Edit question', edit_question_path(question), class: 'col-md-3'\\\\n \\\\n h1= question.title\\\\n h5= question.body\\\\n+p= 'Award available!' if question.award.present?\\\\n br\\\\n+.question_links\\\\n+  -question.links.each do |link|\\\\n+    = content_tag(:p, '', class: \\\\\\\"link-id-#{link.id}\\\\\\\")\\\\n+      -if link.to_gist?\\\\n+        = render partial: 'links/gist', locals: { link: link }\\\\n+      -else\\\\n+        = link_to link.name, link.url\\\\n+      = link_to 'Delete link', link_path(id: link.id, parent_id: question.id), method: :delete, remote: true, class: \\\\\\\"link-id-#{link.id}\\\\\\\"\\\"\"\r\n    ],\r\n    \"comment_positions\": [\r\n      {\r\n        \"original_position\": \"5\",\r\n        \"new_position\": \"5\"\r\n      },\r\n      {\r\n        \"original_position\": \"13\",\r\n        \"new_position\": \"13\"\r\n      },\r\n      {\r\n        \"original_position\": \"20\",\r\n        \"new_position\": \"19\"\r\n      }\r\n    ],\r\n    \"comment_commit_ids\": [\r\n      \"\\\"d2b11db69b1abb5cbe0b41c4c17352504d77c25a\\\"\",\r\n      \"\\\"d2b11db69b1abb5cbe0b41c4c17352504d77c25a\\\"\",\r\n      \"\\\"d2b11db69b1abb5cbe0b41c4c17352504d77c25a\\\"\"\r\n    ],\r\n    \"comment_file_paths\": [\r\n      \"\\\"app/controllers/links_controller.rb\\\"\",\r\n      \"\\\"app/views/answers/_answer.html.slim\\\"\",\r\n      \"\\\"app/views/questions/show.html.slim\\\"\"\r\n    ]\r\n  }")

  test("e2e test") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val inputRDD = sc.parallelize(standardInputList, 1)
    val dataFetch = new DataFetch(sc)
    val inputData = dataFetch.loadJsonInput(session.createDataset(inputRDD)).as[CommentInputData]
    val inputPath = s"$tempPath/input.parquet"
    val outputPath = s"$tempPath/output.csv"
    val outputPathJson = s"$tempPath/output.json"
    val outputPathJsonEscaped = s"$tempPath/output.json_escaped"

    inputData.write.format("parquet").save(inputPath)
    dataFetch.fetch(inputPath, outputPath, None)
    val jsonDump = new JsonDump(sc)
    jsonDump.dump(outputPath, outputPathJson, outputPathJsonEscaped)
    val outputJsonResult = sc.textFile(outputPathJson).collect()(0)
    outputJsonResult should include ("{\"pull_request_url\":")
    val outputJsonResultEscaped = sc.textFile(outputPathJsonEscaped).collect()(0)
    outputJsonResultEscaped should include (
      """{\"pull_request_url\":\"https://api.github.com/repos/CepCap/qna/pulls/7\",""")
  }
}
