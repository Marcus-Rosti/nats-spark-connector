package org.apache.spark.sql.nats

import io.nats.client.Nats
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class NatsSourceProvider extends DataSourceRegister with StreamSourceProvider {

  override def shortName(): String = "nats"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = providerName -> NatsSource.schema

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val (_, ss) = sourceSchema(sqlContext, schema, providerName, parameters)

    val config = NatsSourceConfig(parameters)
    val auth = Nats.credentials(config.jetStreamConfig.credentialsFile)
    val jetStream =
      Nats
        .connect(s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}", auth)
        .jetStream()

    val natsSubscriber = NatsSubscriber(
      jetStream,
      config.subscriptionConfig.streamName,
      config.subscriptionConfig.consumerName,
      config.subscriptionConfig.subjects,
      config.subscriptionConfig.batchSize,
      config.subscriptionConfig.waitTime
    )
    val natsBatchManager = NatsBatchManager(
      natsSubscriber,
      config.natsBatcherConfig.initialDelay,
      config.natsBatcherConfig.batchWaitTime
    )

    val source = NatsSource(sqlContext, natsBatchManager)
    source.start()
    source

  }
}
