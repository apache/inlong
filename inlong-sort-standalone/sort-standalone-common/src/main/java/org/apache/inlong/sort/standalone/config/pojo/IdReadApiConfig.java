package org.apache.inlong.sort.standalone.config.pojo;

// todo 完善config内容
public class IdReadApiConfig {
    private String uid;

    public String getUid() {
        return this.uid;
    }

    public long getAckOverTimeMsForStopPartitionConsumer() {
        return 0L;
    }

    public boolean isAutoStopParitionConsumerForAckOvertime() {
        return false;
    }

    public String getAppName() {
        return null;
    }

    public int getCallbackCorePoolSize() {
        return 0;
    }

    public int getCallbackMaximumPoolSize() {
        return 0;
    }

    public int getCallbackQueueSize() {
        return 0;
    }

    public String getContainerId() {
        return null;
    }

    public String getEnv() {
        return null;
    }

    public String getInstanceName() {
        return null;
    }

    public int getRecvManagerCmdPort() {
        return 0;
    }

    public int getReportStatisticIntervalSec() {
        return 0;
    }

    public String getServerName() {
        return null;
    }

    public String getSetName() {
        return null;
    }

    public boolean getStopConsume() {
        return false;
    }

    public int getTaskQueue() {
        return 0;
    }

    public String getToken() {
        return null;
    }

    public int getUpdateMetaDataIntervalSec() {
        return 0;
    }

    public String getConsumeStrategy() {
        return null;
    }

    public String getInlongGroupId() {
        return null;
    }

    public String getInlongStreamId() {
        return null;
    }

    public String getSortClusterName() {
        return null;
    }

}
