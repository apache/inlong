package org.apache.tubemq.connector.spark

import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

abstract class ConsumerConf {
  private var _group: String = _
  def group: String = _group
  def setGroup(value: String): this.type = {
    _group = value
    this
  }

  private var _topic: String = _
  def topic: String = _topic
  def setTopic(value: String): this.type = {
    _topic = value
    this
  }

  private var _consumeFromMaxOffset: Boolean = true
  def consumeFromMaxOffset: Boolean = _consumeFromMaxOffset
  def setConsumeFromMaxOffset(value: Boolean): this.type = {
    _consumeFromMaxOffset = value
    this
  }

  private var _storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  def storageLevel: StorageLevel = _storageLevel
  def setStorageLevel(value: StorageLevel): this.type = {
    _storageLevel = value
    this
  }
}

class TubeConsumerConf extends ConsumerConf {
  private var _master: String = _
  def master: String = _master
  def setMaster(value: String): this.type = {
    _master = value
    this
  }

  private var _filterAttrs:Array[String] = _
  private var _filterAttrId:String = _
  private[this] var _filterOnRemote: Boolean = false

  def filterAttrId:String = _filterAttrId;
  def filterAttrs:Array[String] = _filterAttrs;

  def setFilterAttrs(attrId: String, value: Array[String]): this.type = {
    require(filterAttrId == null,s"Has been specified $filterAttrId as message filter attribute id")
    _filterAttrs = value
    _filterAttrId = attrId
    this
  }

  def setTids(value: Array[String]): this.type = {
    setFilterAttrs("tid", value)
  }

  def setINames(value: Array[String]): this.type = {
    setFilterAttrs("iname", value)
  }

  private var _includeAttrId:String = null;
  def includeAttrId = _includeAttrId
  def setIncludeAttrId(attrId:String): this.type ={
    _includeAttrId = attrId
    this
  }

  def setIncludeTid(value: Boolean): this.type = {
    if(value){
      setIncludeAttrId("tid")
    }
    this
  }

  def setIncludeIName(value: Boolean): this.type = {
    if(value){
      setIncludeAttrId("iname")
    }
    this
  }

  def filterOnRemote: Boolean = _filterOnRemote

  def setFilterOnRemote(value: Boolean): this.type = {
    _filterOnRemote = value
    this
  }

  // for python api
  def buildFrom(
      master: String,
      group: String,
      topic: String,
      tids: java.util.List[String],
      consumeFromMaxOffset: Boolean,
      includeTid: Boolean,
      storageLevel: StorageLevel): this.type = {
    buildFrom(master, group, topic, tids, "tid", if (includeTid) "tid" else null, consumeFromMaxOffset, storageLevel)
  }

  // for python api
  def buildFrom(
      master: String,
      group: String,
      topic: String,
      filterAttrs: java.util.List[String],
      filterAttrId: String,
      includeAttrId: String,
      consumeFromMaxOffset: Boolean,
      storageLevel: StorageLevel): this.type = {
    buildFrom(master, group, topic, filterAttrs, filterAttrId, false, includeAttrId, consumeFromMaxOffset, storageLevel)
  }

  def buildFrom(
                 master: String,
                 group: String,
                 topic: String,
                 filterAttrs: java.util.List[String],
                 filterAttrId: String,
                 filterOnRemote: Boolean,
                 includeAttrId: String,
                 consumeFromMaxOffset: Boolean,
                 storageLevel: StorageLevel): this.type = {
    _master = master
    setFilterAttrs(filterAttrId, if (filterAttrs != null) filterAttrs.toList.toArray else null)
    setFilterOnRemote(filterOnRemote)
    setIncludeAttrId(includeAttrId)
    setGroup(group)
    setTopic(topic)
    setConsumeFromMaxOffset(consumeFromMaxOffset)
    setStorageLevel(storageLevel)
    this
  }
}

