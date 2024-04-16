package org.apache.spark.sql.nats

import io.nats.client.{JetStream, Message}
import org.apache.spark.internal.Logging

import scala.util.Try

trait NatsPublisher {
  def push(messages: Seq[Message]): Try[Unit]
}

class NatsPublisherImpl(natsClient: JetStream) extends NatsPublisher with Logging {
  override def push(messages: Seq[Message]): Try[Unit] =
    Try(messages.foreach(natsClient.publish))
}

object NatsPublisher {
  def apply(jetStream: JetStream): NatsPublisher = new NatsPublisherImpl(jetStream)
}
