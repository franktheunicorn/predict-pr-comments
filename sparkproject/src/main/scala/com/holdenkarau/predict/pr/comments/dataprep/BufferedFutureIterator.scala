package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Special buffered iterator designed to allow non-blocking consumption
 * of futures from an iterator without evaluating the entire iterator
 * or blocking on long-pole items.
 */

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.collection.Iterator
import scala.util.{Try, Success, Failure}
import scala.concurrent._
import scala.concurrent.duration.Duration

class BufferedFutureIterator[T](
  baseItr: Iterator[Future[T]], bufferSize: Int = 8)
  (implicit ec: ExecutionContext)
    extends Iterator[Option[T]] {

  // Futures which are waiting
  protected val waitingCount = new AtomicInteger()
  // Futures which are ready
  protected val readyBuffer: ArrayBuffer[T] = new ArrayBuffer[T]()

  def hasNext(): Boolean = {
    this.synchronized {
       (waitingCount.get > 0) || ! readyBuffer.isEmpty || baseItr.hasNext
    }
  }

  private def fill(): Unit = {
    while (waitingCount.get + readyBuffer.size < bufferSize && baseItr.hasNext) {
      // Try catch since we don't check hasNext in a sync block
      try {
        // Put the future in the waiting buffer
        val future = this.synchronized {
          val future = baseItr.next()
          waitingCount.getAndIncrement
          future
        }
        // When the future completes move it between buffers
        future.andThen{
          case Success(v) =>
            this.synchronized {
              waitingCount.getAndDecrement
              readyBuffer += v
            }
          case Failure(e) =>
            waitingCount.getAndDecrement
        }
      } catch {
        case e: java.util.NoSuchElementException => None
      }
    }
  }


  def next(): Option[T] = {
    // Special case where we have no buffer
    if (bufferSize <= 0) {
      try {
        Some(Await.result(baseItr.next(), Duration.Inf))
      } catch {
        case _ => None
      }
    // Normal run path
    } else if (hasNext()) {
      fill()
      var resultOpt: Option[T] = None
      // An elem might fail in which case has next _could_ go to false even if we started
      // with true, then we just want to return None
      while (hasNext() && resultOpt.isEmpty) {
        fill()
        Thread.sleep(1)
        this.synchronized {
          if (!readyBuffer.isEmpty) {
            val result = readyBuffer.head
            readyBuffer -= result
            resultOpt = Some(result)
          }
        }
      }
      resultOpt
    } else {
      throw new java.util.NoSuchElementException("Out of elements")
    }
  }
}

