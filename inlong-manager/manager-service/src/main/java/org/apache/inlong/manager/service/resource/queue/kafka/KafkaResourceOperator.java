package org.apache.inlong.manager.service.resource.queue.kafka;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;

/**
 * @author zcy
 * @Date 2022/8/6 18:58
 * @Version 1.0
 */
public class KafkaResourceOperator implements QueueResourceOperator {

  @Override
  public boolean accept(String mqType) {
    return MQType.KAFKA.equals(mqType);
  }

  @Override
  public void createQueueForGroup(@NotNull InlongGroupInfo groupInfo, @NotBlank String operator) {

  }

  @Override
  public void deleteQueueForGroup(InlongGroupInfo groupInfo, String operator) {

  }

  @Override
  public void createQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
      String operator) {

  }

  @Override
  public void deleteQueueForStream(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
      String operator) {

  }
}
