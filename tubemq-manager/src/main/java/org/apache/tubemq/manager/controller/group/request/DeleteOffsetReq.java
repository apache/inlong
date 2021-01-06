package org.apache.tubemq.manager.controller.group.request;

import lombok.Data;
import org.apache.tubemq.manager.controller.node.request.BaseReq;

@Data
public class DeleteOffsetReq extends BaseReq {
    private String groupName;
    private String modifyUser;
    private String topicName;
    private Boolean onlyMemory;
}
