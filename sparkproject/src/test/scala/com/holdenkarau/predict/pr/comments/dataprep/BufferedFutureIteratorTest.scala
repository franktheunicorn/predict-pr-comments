package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * A simple test for everyone's favourite wordcount example.
 */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import scala.concurrent._

class BufferedFutureIteratorTest extends FunSuite {

  test("buffered future iterator empty input") {
    val inputs = List(List[Int]())
    BufferedFutureIteratorTest.testReady(inputs)
  }

  test("buffered future iterator non-empty input") {
    val inputs = List(List[Int](1))
    BufferedFutureIteratorTest.testReady(inputs)
  }

  test("buffered future iterator non-empty input, unique inputs") {
    val inputs = List(1.to(20).toList)
    BufferedFutureIteratorTest.testReady(inputs)
  }

  test("buffered future iterator non-empty input with duplicates") {
    val inputs = List(List[Int](1, 1, 2, 3, 5, 5, 6, 7, 8, 9))
    BufferedFutureIteratorTest.testReady(inputs, bufferSize = 4)
  }
}

object BufferedFutureIteratorTest {
  def testReady[T](inputs: List[List[T]], bufferSize: Int = 8): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val iterators = inputs.map(_.toIterator)
    val futureIterators = iterators.map(itr => itr.map(Future.successful(_)))
    val bufferedIterators = futureIterators.map(
      new BufferedFutureIterator(_, bufferSize=bufferSize))
    val results = bufferedIterators.map(_.toList)
    val compare = inputs.zip(results)
    compare.foreach{case (origin, result) =>
      origin should contain theSameElementsAs result}
  }

}

object BufferedFutureIteratorSpecification extends Properties("BFIS") {
  property("same elems ints") = forAll {(a: List[Int], b: Int) =>
    BufferedFutureIteratorTest.testReady(List(a), 1)
    true
  }
}
