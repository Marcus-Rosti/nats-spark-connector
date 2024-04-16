package org.apache.spark.sql.nats

import io.nats.client.Message
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}

import java.util
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._

import scala.collection.convert.ImplicitConversions._
import scala.util.Try

case class NatsMessageRow(
    subject: Option[String],
    replyTo: Option[String],
    content: Option[Array[Byte]],
    headers: Option[Map[String, Seq[String]]]
)
object MessageToSparkRow {
  def apply(message: Message): NatsMessageRow = {
    val headers: Map[String, Seq[String]] =
      message
        .getHeaders
        .entrySet()
        .toSet
        .map((me: util.Map.Entry[String, util.List[String]]) =>
          (me.getKey, me.getValue.asScala))
        .toMap

    NatsMessageRow(
      Option(message.getSubject),
      Option(message.getReplyTo),
      Option(message.getData),
      Option(headers)
    )
  }
}

object NatsSource {
  val schema: StructType = StructType(
    Array(
      StructField("subject", StringType, nullable = true),
      StructField("replyTo", StringType, nullable = true),
      StructField("content", ArrayType(ByteType), nullable = true),
      StructField("headers", MapType(StringType, ArrayType(StringType)), nullable = true)
    )
  )

  def apply(sqlContext: SQLContext, natsBatchManager: NatsBatchManager): NatsSource =
    new NatsSource(sqlContext, natsBatchManager)
}

class NatsSource(sqlContext: SQLContext, natsBatchManager: NatsBatchManager)
    extends Source
    with Logging {
  import sqlContext.implicits._

  override def schema: StructType = NatsSource.schema

  private def natsBatchToDF(batches: Seq[Message]): DataFrame =
    sqlContext.createDataset(batches.map(MessageToSparkRow(_))).to(NatsSource.schema)

  override def getOffset: Option[Offset] = natsBatchManager.currentBatch.map(LongOffset(_))

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    (start, end) match {
      case (Some(LongOffset(startOff)), LongOffset(endOff)) =>
        natsBatchToDF(natsBatchManager.batchesBetween(Option(startOff), endOff))
      case (None, LongOffset(endOff)) =>
        natsBatchToDF(natsBatchManager.batchesBetween(Option.empty, endOff))
      case (Some(SerializedOffset(json)), offset: Offset) =>
        getBatch(Try(LongOffset(json.toLong)).toOption, offset)
      case (maybeOffset: Option[Offset], offset: SerializedOffset) =>
        getBatch(maybeOffset, LongOffset(offset))
      case (maybeOffset, offset) =>
        logError(s"Invalid offsets in getBatch ($maybeOffset, $offset)")
        sqlContext.createDataset(Seq.empty[NatsMessageRow]).toDF()

    }
  }

  override def commit(end: Offset): Unit = {
    end match {
      case LongOffset(offset) => natsBatchManager.commit(offset)
      case so: SerializedOffset => commit(LongOffset(so))
      case offset: Offset => throw new Exception(s"Invalid Offset :: $offset")
    }
  }

  override def stop(): Unit = natsBatchManager.shutdown()

  def start(): Unit = natsBatchManager.start()
}
