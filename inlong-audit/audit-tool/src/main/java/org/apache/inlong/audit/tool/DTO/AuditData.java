package org.apache.inlong.audit.tool.DTO;

import lombok.Data;

@Data
public class AuditData {
    private int id;
    private String ip;
    private String dockerId;
    private String threadId;
    private long sdkTs;
    private long packetId;
    private long logTs;
    private String groupId;
    private String streamId;
    private String auditId;
    private String auditTag;
    private long auditVersion;
    private long count;
    private long size;
    private long delay;
    private long updateTime;

    public double getSize() {
        return size;
    }

    public double getDataLossRate() {

        return count > 0 ? (double) (count - size) / count : 0.0;
    }

    public long getDataLossCount() {
        // 示例实现，具体逻辑可能需要根据业务需求调整
        return count - size;
    }

    public long getAuditCount() {
        return count;
    }

    public long getExpectedCount() {
        // 示例实现，具体逻辑可能需要根据业务需求调整
        return count;
    }

    public long getReceivedCount() {
        // 示例实现，具体逻辑可能需要根据业务需求调整
        return size;
    }
}