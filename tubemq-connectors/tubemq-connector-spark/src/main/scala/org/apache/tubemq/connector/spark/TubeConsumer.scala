package org.apache.tubemq.connector.spark

import java.util

import org.apache.spark.SparkException
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.apache.tubemq.client.config.{ConsumerConfig, TubeClientConfig}
import org.apache.tubemq.client.consumer.MessageListener
import org.apache.tubemq.client.factory.{MessageSessionFactory, TubeSingleSessionFactory}
import org.apache.tubemq.corebase.Message

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TubeConsumer(
    master: String,
    group: String,
    topic: String,
    filterAttrs: Seq[String],
    filterAttrId:String,
    filterOnRemote:Boolean,
    includeAttrId:String,
    consumeFromMaxOffset: Boolean,
    storageLevel: StorageLevel)
  extends Receiver[Array[Byte]](storageLevel) {
  self =>

  def this(receiverConfig: TubeConsumerConf) = {
    this(receiverConfig.master,
      receiverConfig.group,
      receiverConfig.topic,
      receiverConfig.filterAttrs,
      receiverConfig.filterAttrId,
      receiverConfig.filterOnRemote,
      receiverConfig.includeAttrId,
      receiverConfig.consumeFromMaxOffset,
      receiverConfig.storageLevel)
  }

  require(master != null, "'master' must be set.")
  require(group != null, "'group' must be set.")
  require(topic != null, "'topic' must be set.")



  @transient
  var messageSessionFactory: MessageSessionFactory = null

  override def onStart(): Unit = {
    val config = new TubeClientConfig(master)
    messageSessionFactory = new TubeSingleSessionFactory(config)
    val consumerConfig = new ConsumerConfig(master, group)
    val consumeModel = if (consumeFromMaxOffset) 1 else 0
    consumerConfig.setConsumeModel(consumeModel)
    val consumer = messageSessionFactory.createPushConsumer(consumerConfig)
    if (filterOnRemote && filterAttrs != null) {
      consumer.subscribe(topic, new util.TreeSet[String](filterAttrs), new SimpleMessageListener(topic, filterAttrs,
        filterAttrId, includeAttrId))
    } else {
      consumer.subscribe(topic, null, new SimpleMessageListener(topic, filterAttrs, filterAttrId, includeAttrId))
    }
    consumer.completeSubscribe()
  }

  override def onStop(): Unit = {
    if (messageSessionFactory != null) {
      messageSessionFactory.shutdown()
      messageSessionFactory = null
    }
  }

  class SimpleMessageListener(topic: String, filterAttrs: Seq[String], filterAttrId: String, includeAttrId: String)
    extends MessageListener {

    override def receiveMessages(messages: util.List[Message]): Unit = {
      messages.asScala.foreach(msg => processMessage(msg))
    }

    def processMessage(message: Message) = {
      if (!topic.equals(message.getTopic)) {
        throw new SparkException(
          s"topic error, the message topic is ${message.getTopic}")
      }

      // Deal with message.getData
    }

    override def getExecutor = null

    override def stop() = {}
  }
}
