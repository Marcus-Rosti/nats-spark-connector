package org.apache.spark.sql.nats

import org.apache.spark.sql.nats

import scala.concurrent.duration._

case class JetStreamConfig(host: String, port: Int, credentialsFile: String)
case class SubscriptionConfig(
    streamName: String,
    consumerName: String,
    subjects: Seq[String],
    batchSize: Int,
    waitTime: Duration)
case class NatsBatcherConfig(
    initialDelay: Duration,
    batchWaitTime: Duration
)
case class NatsSourceConfig(
    jetStreamConfig: JetStreamConfig,
    subscriptionConfig: SubscriptionConfig,
    natsBatcherConfig: NatsBatcherConfig
)

// TODO(mrosti): cats.Validated or PureConfig pls
object NatsSourceConfig {
  private def required(config: Map[String, String])(key: String): String =
    config.getOrElse(key, throw new IllegalArgumentException(s"Please specify '$key'"))
  private def default(config: Map[String, String])(key: String, default: String): String =
    config.getOrElse(key, default)

  def apply(config: Map[String, String]): NatsSourceConfig = {
    def requiredKey(key: String): String = required(config)(key)
    def defaultKey(key: String, dflt: String): String = default(config)(key, dflt)

    // JS required
    val host = requiredKey(SourceJSHostOption)
    val port = requiredKey(SourceJSPortOption).toInt
    val credFile = requiredKey(SourceJSCredentialFileOption)

    // Subscriptions required
    val streamName = requiredKey(SourceSubscriptionNameOption)
    val consumerName = requiredKey(SourceSubscriptionConsumerOption)

    // Subscriptions optional
    val subjects = defaultKey(SourceSubscriptionSubjectsOption, "").split(",").map(_.strip())
    val batchSize = defaultKey(SourceSubscriptionBatchSizeOption, "100").toInt
    val waitTime = defaultKey(SourceSubscriptionWaitTimeOption, "60").toInt.seconds

    // Read batch options
    val initialDelay = defaultKey(SourceBatchInitialDelayOption, "60").toInt.seconds
    val schedulerWaitTime = defaultKey(SourceBatchWaitTimeOption, "30").toInt.seconds

    NatsSourceConfig(
      JetStreamConfig(host, port, credFile),
      SubscriptionConfig(
        streamName,
        consumerName,
        subjects,
        batchSize,
        waitTime
      ),
      NatsBatcherConfig(initialDelay, schedulerWaitTime)
    )
  }
}
