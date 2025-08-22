// manager-dao/src/main/java/org/apache/inlong/manager/dao/entity/AuditAlertRuleEntity.java
package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.util.Date;

@Data
public class AuditAlertRuleEntity {
    private Integer id;
    private String inlongGroupId;
    private String inlongStreamId;
    private String auditId;
    private String alertName;
    private String condition;
    private String level;
    private String notifyType;
    private String receivers;
    private Boolean enabled;
    private String creator;
    private String modifier;
    private Date createTime;
    private Date updateTime;
}