package org.apache.tubemq.connector.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkEnv
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions

object TubeFunctions {

  class ArrayByteRDDFunctins(rdd: RDD[Array[Byte]]) extends Serializable {
    def saveToTube(config: ProducerConf): Unit = {
      SparkEnv.get.conf.set("spark.task.maxFailures", "1")
      SparkEnv.get.conf.set("spark.speculation", "false")
      config match {
        case conf: TubeProducerConf =>
          val sender = new TubeProducer(conf)
          val writer = (iter: Iterator[Array[Byte]]) => {
            sender.start()
            try {
              while (iter.hasNext) {
                val record = iter.next()
                sender.send(record)
              }
            } finally {
              sender.close()
            }
          }
          rdd.context.runJob(rdd, writer)

        case _ => throw new UnsupportedOperationException("Unknown SenderConfig")
      }
    }
  }

  class StringRDDFunctins(rdd: RDD[String]) extends Serializable {
    def saveToTube(config: ProducerConf): Unit = {
      rdd.map(_.getBytes("UTF-8")).saveToTube(config)
    }
  }

  class ArrayByteDStreamFunctins(dstream: DStream[Array[Byte]]) extends Serializable {
    def saveRDD(rdd: RDD[Array[Byte]], sender: TubeProducer): Unit = {
      val writer = (iter: Iterator[Array[Byte]]) => {
        try {
          sender.start()
          while (iter.hasNext) {
            val record = iter.next()
            sender.send(record)
          }
        }
        finally {
          sender.close()
        }
      }
      rdd.context.runJob(rdd, writer)
    }

    def saveToTube(config: ProducerConf): Unit = {
      SparkEnv.get.conf.set("spark.task.maxFailures", "1")
      SparkEnv.get.conf.set("spark.speculation", "false")
      val sender: TubeProducer = config match {
        case conf: TubeProducerConf => new TubeProducer(conf)
        case _ => throw new UnsupportedOperationException("Unknown SenderConfig")
      }
      val saveFunc = (rdd: RDD[Array[Byte]], time: Time) => {
        saveRDD(rdd, sender)
      }
      dstream.foreachRDD(saveFunc)
    }
  }

  class StringDStreamFunctins(dstream: DStream[String]) extends Serializable {
    def saveToTube(config: ProducerConf): Unit = {
      dstream.map(_.getBytes("UTF-8")).saveToTube(config)
    }
  }

  implicit def rddToArrayByteRDDFunctions(rdd: RDD[Array[Byte]]): ArrayByteRDDFunctins = {
    new ArrayByteRDDFunctins(rdd)
  }

  implicit def rddToStringRDDFunctions(rdd: RDD[String]): StringRDDFunctins = {
    new StringRDDFunctins(rdd)
  }

  implicit def dStreamToArrayByteDStreamFunctions(
      dstream: DStream[Array[Byte]]): ArrayByteDStreamFunctins = {
    new ArrayByteDStreamFunctins(dstream)
  }

  implicit def dStreamToStringDStreamFunctions(
      dstream: DStream[String]): StringDStreamFunctins = {
    new StringDStreamFunctins(dstream)
  }
}
