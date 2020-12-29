package org.apache.tubemq.manager.controller.topic.request;

import lombok.Data;
import org.apache.tubemq.manager.controller.node.request.BaseReq;

@Data
public class RebalanceConsumerReq extends BaseReq {
    public String groupName;
    public String confModAuthToken;
    public Integer reJoinWait;
    public String modifyUser;
    public String consumerId;
}
