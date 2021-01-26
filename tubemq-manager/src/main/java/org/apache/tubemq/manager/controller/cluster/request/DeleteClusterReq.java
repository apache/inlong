package org.apache.tubemq.manager.controller.cluster.request;

import lombok.Data;

@Data
public class DeleteClusterReq {
    private Integer clusterId;
    private String modifyUser;
}
