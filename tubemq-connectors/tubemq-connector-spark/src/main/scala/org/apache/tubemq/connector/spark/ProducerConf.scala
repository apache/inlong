package org.apache.tubemq.connector.spark

// Hold sender common parameters
abstract class ProducerConf extends Serializable {
}

class TubeProducerConf extends ProducerConf {
  private var _topic: String = _
  def topic: String = _topic
  def setTopic(value: String): this.type = {
    _topic = value
    this
  }

  private var _masterHostAndPort: String = _
  def masterHostAndPort: String = _masterHostAndPort
  def setMasterHostAndPort(value: String): this.type = {
    _masterHostAndPort = value
    this
  }

  private var _timeout: Long = 20000 // 20s
  def timeout: Long = _timeout
  def setTimeout(value: Long): this.type = {
    _timeout = value
    this
  }

  private var _maxRetryTimes: Int = 3
  def maxRetryTimes: Int = _maxRetryTimes
  def setMaxRetryTimes(value: Int): this.type = {
    _maxRetryTimes = value
    this
  }


  private var _exitOnException: Boolean = true
  def exitOnException: Boolean = _exitOnException
  def setExitOnException(value: Boolean): this.type = {
    _exitOnException = value
    this
  }

  // for python api
  def buildFrom(
      topic: String,
      masterHostAndPort: String,
      timeout: Int,
      maxRetryTimes: Int,
      exitOnException: Boolean): this.type = {
    _topic = topic
    _masterHostAndPort = masterHostAndPort
    _timeout = timeout
    _maxRetryTimes = maxRetryTimes
    _exitOnException = exitOnException
    this
  }
}
