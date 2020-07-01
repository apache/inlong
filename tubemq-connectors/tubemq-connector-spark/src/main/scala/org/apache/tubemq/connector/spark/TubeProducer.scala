package org.apache.tubemq.connector.spark

import java.util.HashSet

import org.apache.spark.SparkException

import org.apache.tubemq.client.config.TubeClientConfig
import org.apache.tubemq.client.factory.TubeSingleSessionFactory
import org.apache.tubemq.client.producer.{MessageProducer, MessageSentCallback, MessageSentResult}
import org.apache.tubemq.corebase.Message
import org.slf4j.{Logger, LoggerFactory}

private[spark] class TubeProducer(
    topic: String,
    masterHostAndPort: String,
    maxRetryTimes: Int,
    exitOnException: Boolean)
  extends Serializable {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[TubeProducer])

  private var producer: MessageProducer = _
  private var sessionFactory: TubeSingleSessionFactory = _

  def this(producerConfig: TubeProducerConf) = {
    this(producerConfig.topic, producerConfig.masterHostAndPort, producerConfig.maxRetryTimes, producerConfig.exitOnException)
  }

  def start(): Unit = {
    val clientConfig = new TubeClientConfig(masterHostAndPort)
    sessionFactory = new TubeSingleSessionFactory(clientConfig)
    producer = sessionFactory.createProducer()
    val hashSet: HashSet[String] = new HashSet[String]
    hashSet.add(topic)
    producer.publish(hashSet)
  }

  def send(body: Array[Byte]): Unit = {
    val message: Message = new Message(topic, body)
    producer.sendMessage(message, new MessageSentCallback {
      override def onMessageSent(sendResult: MessageSentResult): Unit = {
        if (!sendResult.isSuccess) {
          // rollback to sync
          sendSync(message)
        }
      }

      override def onException(throwable: Throwable): Unit = {
        if (exitOnException) {
          throw new SparkException("Sender message exception", throwable)
        } else {
          LOG.warn("Sender message exception", throwable)
        }
      }

    })
  }

  private def sendSync(message: Message): Unit = {
    var success = false
    var retryTimes = 0
    while (!success && retryTimes < maxRetryTimes) {
      retryTimes += 1
      val sendResult = producer.sendMessage(message)
      if (sendResult.isSuccess) {
        success = true
      }
      else {
        retryTimes = maxRetryTimes
      }
    }

    if (!success) {
      val error = s"Sender message exception: exceed maxRetryTimes($maxRetryTimes)"
      if (exitOnException) {
        throw new SparkException(error)
      } else {
        LOG.warn(error)
      }
    }
  }

  def close(): Unit = {
    try {
      if (producer != null) {
        producer.shutdown
        producer = null
      }
      if (sessionFactory != null) {
        sessionFactory.shutdown
        sessionFactory = null
      }
    }
    catch {
      case e: Exception => {
        LOG.error("Shutdown producer error", e)
      }
    }
  }

}
