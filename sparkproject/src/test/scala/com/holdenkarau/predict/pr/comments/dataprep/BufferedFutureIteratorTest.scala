package com.holdenkarau.predict.pr.comments.sparkProject.dataprep

/**
 * A simple test for everyone's favourite wordcount example.
 */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalacheck._
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

  def testReadyWithDelays[T](inputs: List[(T, Int)], bufferSize: Int = 8): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val iterator = inputs.toIterator
    val futureIterator = iterator.map{
      case (elem, delay) =>
        Future {
          Thread.sleep(delay)
          elem
        }
    }
    val bufferedIterator = new BufferedFutureIterator(futureIterator,
      bufferSize=bufferSize)
    val results = bufferedIterator.toList
    val original = inputs.map(_._1)
    original should contain theSameElementsAs results
  }

}

object BufferedFutureIteratorSpecification extends Properties("BFIS") {
  property("same elems ints ready right away") = forAll {(a: List[Int], b: Int) =>
    BufferedFutureIteratorTest.testReady(List(a), b)
    true
  }

  property("same elems string ready right away") = forAll {(a: List[String], b: Int) =>
    BufferedFutureIteratorTest.testReady(List(a), b)
    true
  }

  val bufferGen = Gen.choose(8, 100)
  val elemGen = for {
    str <- Arbitrary.arbitrary[String]
    delay <- Gen.choose(0, 50)
  } yield (str, delay)
  val elemListGen = Gen.containerOf[List, (String, Int)](elemGen)

  property("same elems string wiht delays away") = forAll(
    elemListGen, bufferGen) {
    (elems: List[(String, Int)], buffer: Int) =>
    BufferedFutureIteratorTest.testReadyWithDelays(elems, buffer)
    true
  }
}
