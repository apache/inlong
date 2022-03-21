package org.apache.inlong.tubemq.manager.controller.cluster.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ClusterCountVo {
    private Long clusterId;
    private String clusterName;
    private String masterIp;
    private int reloadBrokerSize;
    private int brokerCount;
    private int topicCount;
    private int storeCount;
    private int partitionCount;
    private int consumerGroupCount;
    private int consumerCount;
}
