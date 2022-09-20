package org.apache.inlong.manager.client.cli.pojo;

import lombok.Data;

/**
 * Cluster info, including cluster name, cluster type, etc.
 */
@Data
public class ClusterInfo {

    private int id;
    private String name;
    private String type;
    private String url;
    private String clusterTags;
    private Integer status;
}
