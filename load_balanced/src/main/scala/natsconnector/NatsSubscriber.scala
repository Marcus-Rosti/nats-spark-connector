package natsconnector

import java.util.concurrent.TimeUnit
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.{ConsumerConfiguration, PublishAck}
import io.nats.client.{PullSubscribeOptions, PushSubscribeOptions}
import io.nats.client.JetStreamSubscription

import java.time.Duration
import java.util
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

class NatsSubscriber() {
  val subjects:String = NatsConfigSource.config.streamSubjects.get
  //val deliverySubject:String = NatsConfigSource.config.queueDeliverySubject
  //val queue:String = NatsConfigSource.config.queue
  val js:JetStream = NatsConfigSource.config.js.get
  val nc:Connection = NatsConfigSource.config.nc.get
  val messageReceiveWaitTime:Duration = NatsConfigSource.config.messageReceiveWaitTime
  val durable:Option[String] = NatsConfigSource.config.durable
  val streamName = NatsConfigSource.config.streamName.get
  val fetchBatchSize = NatsConfigSource.config.msgFetchBatchSize

  val jSub: JetStreamSubscription = {
    val subjectArray = this.subjects.replace(" ", "").split(",")



      val pso = {
        val config = PullSubscribeOptions.builder()
          .stream(this.streamName)
        if(this.durable.isDefined) {
          config.durable(s"${this.durable.get}")
          config.bind(true)
        } else {
          val cco = ConsumerConfiguration.builder()
            .filterSubjects(subjectArray.toList.asJava)
            .build()
          config.configuration(cco)
        }
        config.build()
      }
    js.subscribe(null, pso)
  }

  def pullNext():List[Message] = {
    //var msgArray:Array[Message] = null
    // println(s"Subscription is active:${jSub.isActive()}")

    try {
      this.jSub.fetch(this.fetchBatchSize, this.messageReceiveWaitTime).asScala.toList
    } catch {
      case ex: InterruptedException => println(s"nextMessage() waitTime exceeded: ${ex.getMessage()}."); List.empty[Message]
      case ex: IllegalStateException => println(s"Disregarding NATS msg: ${ex.getMessage()}"); new util.ArrayList[Message](); List.empty[Message]
    }
  }

  def unsubscribe():Unit = {
    jSub.drain(Duration.ofSeconds(1))
  }
}



