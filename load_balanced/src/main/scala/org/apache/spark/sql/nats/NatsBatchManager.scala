package org.apache.spark.sql.nats

import io.nats.client.Message
import org.apache.spark.internal.Logging

import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

case class NatsBatch(id: Long, messages: Seq[Message])

trait NatsBatchManager {
  def commit(id: Long): Try[Unit]
  def commitBatches(end: Long): Try[Unit]
  def batchesBetween(start: Option[Long], end: Long): Seq[Message]
  def currentBatch: Option[Long]
  def shutdown(): Unit
  def start(): Unit
}

class NatsBatchManagerImpl(
    commitTracker: TrieMap[Long, Seq[Message]],
    subscriber: NatsSubscriber,
    initialDelay: Duration,
    batchWaitTime: Duration,
    threadPool: ScheduledExecutorService)
    extends NatsBatchManager
    with Logging {

  override def start(): Unit = {
    val periodicLookup: Runnable = () => {
      logTrace("Running pull")
      pullBatch
    }
    val scheduler = threadPool.scheduleAtFixedRate(
      periodicLookup,
      initialDelay.toSeconds,
      batchWaitTime.toSeconds,
      TimeUnit.SECONDS)

    threadPool.scheduleAtFixedRate(
      periodicLookup,
      initialDelay.toSeconds,
      batchWaitTime.toSeconds,
      TimeUnit.SECONDS)
  }

  private val beginningOfTime: Long = -1
  private val counter: AtomicLong = new AtomicLong(beginningOfTime)

  override def commit(id: Long): Try[Unit] = for {
    messages <- commitTracker
      .get(id)
      .toRight(new NoSuchElementException(s"No tracked batch for $id"))
      .toTry
    _ <- Try(messages.foreach(_.ack()))
    _ <- commitTracker
      .remove(id)
      .fold[Try[Seq[Message]]](
        Failure(new NoSuchElementException(s"Failed to remove batch $id")))(Try(_))
    _ = logTrace(s"Commited: $id")
  } yield {}

  private def addToMap(messages: Seq[Message]): Option[NatsBatch] = {
    val idx = counter.incrementAndGet()
    logTrace(s"Upserting batch $idx with ${messages.size} messages")
    commitTracker.put(idx, messages).map(NatsBatch(idx, _))
  }

  private def pullBatch: Try[Unit] = for {
    messages <- subscriber.pull
    _ <- addToMap(messages).toRight(new Exception("Failed to add to batch map")).toTry
  } yield {}

  override def commitBatches(id: Long): Try[Unit] = Try {
    for {
      commitId <- commitTracker.keySet
      if commitId <= id
    } yield {
      commit(commitId)
    }
  }

  override def batchesBetween(start: Option[Long], end: Long): Seq[Message] = {
    val startOrAll: Long = start.getOrElse(beginningOfTime)
    commitTracker
      .keySet
      .filter(t => startOrAll <= t && t <= end)
      .flatMap(commitTracker.get)
      .toSeq
      .flatten
  }

  override def currentBatch: Option[Long] = counter.get() match {
    case idx if idx == beginningOfTime => Option.empty
    case idx => Option(idx)
  }

  override def shutdown(): Unit = {
    // Stop querying
    threadPool.shutdown()

    // nack incomplete batches
    val batches = commitTracker.values.toSeq.flatten
    commitTracker.clear()
    batches.foreach(_.nak())
  }
}

object NatsBatchManager {
  private val numberOfThreads = 1
  def apply(
      subscriber: NatsSubscriber,
      initialDelay: Duration,
      batchWaitTime: Duration
  ): NatsBatchManager = {
    val threadPool = new ScheduledThreadPoolExecutor(numberOfThreads)
    val commitTracker: TrieMap[Long, Seq[Message]] = TrieMap.empty
    new NatsBatchManagerImpl(commitTracker, subscriber, initialDelay, batchWaitTime, threadPool)
  }
}
