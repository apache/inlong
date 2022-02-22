package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class StreamSourceEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer id;
    private String inlongGroupId;
    private String inlongStreamId;
    private String sourceType;
    private String agentIp;
    private String uuid;

    private Integer serverId;
    private String serverName;
    private Integer clusterId;
    private String clusterName;
    private String heartbeat;

    // extParams saved filePath, fileRollingType, dbName, tableName, etc.
    private String extParams;

    private Integer status;
    private Integer previousStatus;
    private Integer isDeleted;
    private String creator;
    private String modifier;
    private Date createTime;
    private Date modifyTime;

}