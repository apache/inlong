package org.apache.inlong.sort.base.metric.sub;

import org.apache.inlong.sort.base.metric.SinkMetricData;

import java.util.Map;

public interface SinkSubMetricData {

    /**
     * Get sub sink metric map
     *
     * @return The sub sink metric map
     */
    Map<String, SinkMetricData> getSubSourceMetricMap();

}
