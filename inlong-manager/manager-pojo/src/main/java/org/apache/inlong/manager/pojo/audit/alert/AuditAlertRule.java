// manager-pojo/src/main/java/org/apache/inlong/manager/pojo/audit/alert/AuditAlertRule.java
package org.apache.inlong.manager.pojo.audit.alert;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.Date;

@Data
@ApiModel("Audit告警策略配置")
public class AuditAlertRule {
    @ApiModelProperty("策略ID")
    private Integer id;

    @ApiModelProperty("关联的InLong Group ID")
    private String inlongGroupId;

    @ApiModelProperty("关联的InLong Stream ID")
    private String inlongStreamId;

    @ApiModelProperty("审计ID（关联具体审计指标，如发送成功数、延迟等）")
    private String auditId;

    @ApiModelProperty("告警名称")
    private String alertName;

    @ApiModelProperty("触发条件（如count>10000、delay>60000）")
    private String condition; // 可使用表达式，如"count > 10000 && delay > 60000"

    @ApiModelProperty("告警级别（INFO/WARN/ERROR）")
    private String level;

    @ApiModelProperty("通知方式（EMAIL/SMS/HTTP）")
    private String notifyType;

    @ApiModelProperty("通知接收者（多个用逗号分隔）")
    private String receivers;

    @ApiModelProperty("是否启用")
    private Boolean enabled;

    @ApiModelProperty("创建时间")
    private Date createTime;

    @ApiModelProperty("更新时间")
    private Date updateTime;
}