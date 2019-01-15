package com.holdenkarau.predict.pr.comments.sparkProject

/**
 * Special buffered iterator designed to allow non-blocking consumption
 * of futures from an iterator without evaluating the entire iterator
 * or blocking on long-pole items.
 */

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
  protected val waitingBuffer: ArrayBuffer[Future[T]] = new ArrayBuffer[Future[T]]()
  // Futures which are ready
  protected val readyBuffer: ArrayBuffer[Future[T]] = new ArrayBuffer[Future[T]]()

  def hasNext(): Boolean = {
    this.synchronized {
      ! readyBuffer.isEmpty || ! waitingBuffer.isEmpty || baseItr.hasNext
    }
  }

  private def fill(): Unit = {
    while (waitingBuffer.size + readyBuffer.size < bufferSize && baseItr.hasNext) {
      // Try catch since we don't check hasNext in a sync block
      try {
        // Put the future in the waiting buffer
        val future = this.synchronized {
          val future = baseItr.next()
          waitingBuffer += future
          future
        }
        // When the future completes move it between buffers
        def completeFun(f: Try[T]): Unit = {
          this.synchronized {
            readyBuffer += future
            waitingBuffer -= future
          }
        }
        future onComplete completeFun
      } catch {
        case e: java.util.NoSuchElementException => None
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
        // Wait for a future to finish
        if (readyBuffer.isEmpty && !waitingBuffer.isEmpty) {
          Future.firstCompletedOf(waitingBuffer)
        }
        this.synchronized {
          if (!readyBuffer.isEmpty) {
            val result = readyBuffer.head
            readyBuffer -= result
            resultOpt = Some(result)
          }
        }
      }
    Await.result(resultOpt.get, Duration.Inf)
    } else {
      throw new java.util.NoSuchElementException("Out of elements")
    }
  }
}

