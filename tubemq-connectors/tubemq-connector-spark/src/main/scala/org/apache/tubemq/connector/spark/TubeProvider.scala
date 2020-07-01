package org.apache.tubemq.connector.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import TubeFunctions._

class TubeProvider(val ssc: StreamingContext) {

  /**
   * Receive from tube
   * @param config the configuration of receiver's consumer
   * @param numReceiver the number of receivers
   * @return DStream[ Array[Byte] ]
   */
  def bytesStream(
      config: ConsumerConf,
      numReceiver: Int): DStream[Array[Byte]] = {
    require(numReceiver >= 1, s"the argument 'numReceiver' error: $numReceiver >= 1 ?")
    val streams = config match {
      case conf: TubeConsumerConf =>
        (1 to numReceiver).map { _ =>
          ssc.receiverStream(new TubeConsumer(conf))
        }
      case _ =>
        throw new UnsupportedOperationException("Unknown receiver config.")
    }
    ssc.union(streams)
  }

  /**
   * Receive from tube
   * @param config the configuration of receiver's consumer
   * @param numReceiver the number of receivers
   * @return DStream[String]
   */
  def textStream(
      config: ConsumerConf,
      numReceiver: Int): DStream[String] = {
    bytesStream(config, numReceiver).map(x => new String(x, "utf8"))
  }

  /**
   * Send to tube
   * @param dstream the data to be send
   * @param config sender config
   */
  def saveBytesStreamToTDBank(dstream: DStream[Array[Byte]], config: ProducerConf): Unit = {
    dstream.saveToTube(config)
  }

  /**
   * Send to tube
   * @param dstream the data to be send
   * @param config sender config
   */
  def saveTextStreamToTDBank(dstream: DStream[String], config: ProducerConf): Unit = {
    dstream.saveToTube(config)
  }
}

