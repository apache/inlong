package org.apache.inlong.audit.tool.basemetric.vo;



import java.sql.Timestamp;

public class AuditDataVo {
    private String ip;
    private String dockerId;
    private String threadId;
    private Timestamp logTs;
    private String inlongGroupId;
    private String inlongStreamId;
    private String auditId;
    private long count;
    private long size;
    private long delay;

    // Getters and Setters
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDockerId() {
        return dockerId;
    }

    public void setDockerId(String dockerId) {
        this.dockerId = dockerId;
    }

    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public Timestamp getLogTs() {
        return logTs;
    }

    public void setLogTs(Timestamp logTs) {
        this.logTs = logTs;
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId;
    }

    public String getInlongStreamId() {
        return inlongStreamId;
    }

    public void setInlongStreamId(String inlongStreamId) {
        this.inlongStreamId = inlongStreamId;
    }

    public String getAuditId() {
        return auditId;
    }

    public void setAuditId(String auditId) {
        this.auditId = auditId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }
}
