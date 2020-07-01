package org.apache.tubemq.connector.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}

class JavaTubeProvider private(tdbank: TubeProvider) {
  def this(jssc: JavaStreamingContext) = {
    this(new TubeProvider(jssc.ssc))
  }

  def bytesStream(
      config: ConsumerConf,
      numReceiver: Int): JavaDStream[Array[Byte]] = {
    tdbank.bytesStream(config, numReceiver)
  }

  def textStream(
      config: ConsumerConf,
      numReceiver: Int,
      storageLevel: StorageLevel): JavaDStream[String] = {
    tdbank.textStream(config, numReceiver)
  }

  def saveBytesStreamToTDBank(
      dstream: JavaDStream[Array[Byte]],
      config: ProducerConf): Unit = {
    tdbank.saveBytesStreamToTDBank(dstream.dstream, config)
  }

  def saveTextStreamToTDBank(
      dstream: JavaDStream[String],
      config: ProducerConf): Unit = {
    tdbank.saveTextStreamToTDBank(dstream.dstream, config)
  }
}
