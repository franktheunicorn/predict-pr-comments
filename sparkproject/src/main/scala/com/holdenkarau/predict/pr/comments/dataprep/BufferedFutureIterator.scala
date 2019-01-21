package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Special buffered iterator designed to allow non-blocking consumption
 * of futures from an iterator without evaluating the entire iterator
 * or blocking on long-pole items.
 */

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.collection.Iterator
import scala.util.Try
import scala.concurrent._
import scala.concurrent.duration.Duration

class BufferedFutureIterator[T](
  baseItr: Iterator[Future[T]], bufferSize: Int = 8)
  (implicit ec: ExecutionContext)
    extends Iterator[T] {

  // Futures which are waiting
  protected val waitingCount = new AtomicInteger()
  // Futures which are ready
  protected val readyBuffer: ArrayBuffer[Future[T]] = new ArrayBuffer[Future[T]]()

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
        def completeFun(f: Try[T]): Unit = {
          this.synchronized {
            readyBuffer += future
            waitingCount.getAndDecrement
          }
        }
        future onComplete completeFun
      } catch {
        case e: java.util.NoSuchElementException => None
        case _ => None // inet, etc.
      }
    }
  }


  def next(): T = {
    if (bufferSize <= 0) {
      Await.result(baseItr.next(), Duration.Inf)
    } else if (hasNext()) {
      fill()
      var resultOpt: Option[Future[T]] = None
      while (resultOpt.isEmpty) {
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
      // Ignore any problems during waiting
      try {
        Await.result(resultOpt.get, Duration.Inf)
      } catch {
        case _ => None
      }
    } else {
      throw new java.util.NoSuchElementException("Out of elements")
    }
  }
}

