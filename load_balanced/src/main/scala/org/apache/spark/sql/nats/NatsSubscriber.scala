package org.apache.spark.sql.nats

import io.nats.client._
import io.nats.client.api.ConsumerConfiguration
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.time.{Duration => JDuration}

import scala.util.Try

trait NatsSubscriber {
  def pull: Try[Seq[Message]]
}

class NatsSubscriberImpl(
    subscription: JetStreamSubscription,
    batchSize: Int,
    waitTime: Duration)
    extends NatsSubscriber
    with Logging {
  override def pull: Try[Seq[Message]] =
    Try(subscription.fetch(batchSize, JDuration.ofSeconds(waitTime.toSeconds)).asScala)
      .recover {
        case ex: InterruptedException =>
          logError("fetch() waitTime exceeded", ex)
          Seq.empty
        case ex: IllegalStateException =>
          logError("Disregarding NATS msg", ex)
          Seq.empty
      }

}

object NatsSubscriber {
  def apply(
      jetStream: JetStream,
      streamName: String,
      consumerName: String,
      subjects: Seq[String],
      batchSize: Int,
      waitTime: Duration): NatsSubscriber = {
    val pullSubscribeOptions =
      PullSubscribeOptions
        .builder()
        .fastBind(true)
        .stream(streamName)
        .durable(consumerName)
        .configuration(ConsumerConfiguration.builder().filterSubjects(subjects.asJava).build())
        .build()
    val subscription = jetStream.subscribe(None.orNull, pullSubscribeOptions)

    new NatsSubscriberImpl(subscription, batchSize, waitTime)
  }
}
