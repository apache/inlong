package org.apache.inlong.agent.pojo;

import java.util.Map;

/**
 * @description
 * @date: 2022/5/9
 */
public class DebeziumOffset {

    private static final long serialVersionUID = 1L;

    public Map<String, ?> sourcePartition;
    public Map<String, ?> sourceOffset;

    public void setSourcePartition(Map<String, ?> sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public void setSourceOffset(Map<String, ?> sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    @Override
    public String toString() {
        return "DebeziumOffset{"
                + "sourcePartition="
                + sourcePartition
                + ", sourceOffset="
                + sourceOffset
                + '}';
    }
}
