package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

/**
 * Fetch the patches from GitHub
 */
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import com.softwaremill.sttp._
import com.softwaremill.sttp.asynchttpclient.future._

import scala.concurrent._
import scala.concurrent.duration.Duration


class PatchFetcher(sc: SparkContext) {
  val session = SparkSession.builder().getOrCreate()
  import session.implicits._

  /**
   * Fetches the github PR diff's for elements not found in the cache
   * and returns the new patches.
   * Note: this needs new types
   */
  def fetchPatches[T <: HasPatchUrl](inputData: Dataset[T],
    cachedData: Dataset[StoredPatch])
    (implicit e1: Encoder[T], e2: Encoder[(T, StoredPatch)]):
      Dataset[(T, StoredPatch)] = {
    val joinedData = inputData.join(cachedData,
      Seq("pull_request_url"),
      joinType = "left_anti").as[T]
    val result = joinedData.mapPartitions(
      PatchFetcher.fetchPatchesIterator)
    result
  }
}

object PatchFetcher {
  // Note if fetch patch is called inside the root JVM this might result in serilization "fun"
  @transient implicit lazy val sttpBackend = AsyncHttpClientFutureBackend()
  import scala.concurrent.ExecutionContext.Implicits.global

  // Allows us to fetch the patches in parallel
  def fetchPatchesIterator[T<: HasPatchUrl](records: Iterator[T]):
      Iterator[(T, StoredPatch)] = {
    val patchFutures = records.map(fetchPatch)
    val resultFutures = patchFutures.map(future => future.map(processResponse))
    // We use a buffered future iterator since difference requests may complete at different rates
    val result = new BufferedFutureIterator(resultFutures)
      .flatMap(x => x).flatMap(x => x)
    result
  }

  def fetchDiffForPatchUrl(patchUrl: String): Future[Response[String]] = {
    val diffUrl = patchUrl.substring(0, patchUrl.length - 5) + "diff"
    val diffRequest = sttp
      .get(uri"${diffUrl}")
    val diffResponseFuture = diffRequest.send()
    diffResponseFuture
  }

  def fetchPatchForPatchUrl(patchUrl: String): Future[Response[String]] = {
    val patchRequest = sttp
      .get(uri"${patchUrl}")
    val patchResponseFuture = patchRequest.send()
    patchResponseFuture
  }


  def fetchPatchAndDiffForURL(patchUrl: String): (Future[Response[String]], Future[Response[String]]) = {
    val patchResponseFuture = fetchPatchForPatchUrl(patchUrl)
    val diffResponseFuture = fetchDiffForPatchUrl(patchUrl)
    (patchResponseFuture, diffResponseFuture)
  }

  def fetchPatch[T<: HasPatchUrl](record: T):
      Future[(T, Response[String], Response[String])] = {
    try {
      val (patchResponseFuture, diffResponseFuture) = fetchPatchAndDiffForURL(record.pull_patch_url)

      val responseFuture = patchResponseFuture.zip(diffResponseFuture)
      responseFuture.map{case (patch, diff) => (record, patch, diff)}
    } catch {
      case e: Exception => // We can get null pointers and other weird errors trying to fetch
        Future.failed[(T, Response[String], Response[String])](e)
    }
  }

  def processResponse[T<: HasPatchUrl](data: (T, Response[String], Response[String])):
      Option[(T, StoredPatch)] = {
    val (input, patchResponse, diffResponse) = data
    // Skip errors, we have a lot of data
    if (patchResponse.code == StatusCodes.Ok && diffResponse.code == StatusCodes.Ok) {
      Some((input,
        StoredPatch(
          input.pull_request_url,
          patchResponse.unsafeBody,
          diffResponse.unsafeBody)))
    } else {
      None
    }
  }
}
