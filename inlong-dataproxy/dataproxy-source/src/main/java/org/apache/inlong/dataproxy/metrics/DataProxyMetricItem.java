/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Event;
import org.apache.inlong.commons.config.metrics.CountMetric;
import org.apache.inlong.commons.config.metrics.Dimension;
import org.apache.inlong.commons.config.metrics.MetricDomain;
import org.apache.inlong.commons.config.metrics.MetricItem;
import org.apache.inlong.dataproxy.utils.Constants;

/**
 * 
 * DataProxyMetricItem
 */
@MetricDomain(name = "DataProxy")
public class DataProxyMetricItem extends MetricItem {

    public static final String KEY_CLUSTER_ID = "clusterId";
    public static final String KEY_SOURCE_ID = "sourceId";
    public static final String KEY_SOURCE_DATA_ID = "sourceDataId";
    public static final String KEY_INLONG_GROUP_ID = "inlongGroupId";
    public static final String KEY_INLONG_STREAM_ID = "inlongStreamId";
    public static final String KEY_SINK_ID = "sinkId";
    public static final String KEY_SINK_DATA_ID = "sinkDataId";
    //
    public static final String M_READ_SUCCESS_COUNT = "readSuccessCount";
    public static final String M_READ_SUCCESS_SIZE = "readSuccessSize";
    public static final String M_READ_FAIL_COUNT = "readFailCount";
    public static final String M_READ_FAIL_SIZE = "readFailSize";
    public static final String M_SEND_COUNT = "sendCount";
    public static final String M_SEND_SIZE = "sendSize";
    public static final String M_SEND_SUCCESS_COUNT = "sendSuccessCount";
    public static final String M_SEND_SUCCESS_SIZE = "sendSuccessSize";
    public static final String M_SEND_FAIL_COUNT = "sendFailCount";
    public static final String M_SEND_FAIL_SIZE = "sendFailSize";
    //
    public static final String M_SINK_DURATION = "sinkDuration";
    public static final String M_NODE_DURATION = "nodeDuration";
    public static final String M_WHOLE_DURATION = "wholeDuration";

    @Dimension
    public String clusterId;
    @Dimension
    public String sourceId;
    @Dimension
    public String sourceDataId;
    @Dimension
    public String inlongGroupId;
    @Dimension
    public String inlongStreamId;
    @Dimension
    public String sinkId;
    @Dimension
    public String sinkDataId;
    @CountMetric
    public AtomicLong readSuccessCount = new AtomicLong(0);
    @CountMetric
    public AtomicLong readSuccessSize = new AtomicLong(0);
    @CountMetric
    public AtomicLong readFailCount = new AtomicLong(0);
    @CountMetric
    public AtomicLong readFailSize = new AtomicLong(0);
    @CountMetric
    public AtomicLong sendCount = new AtomicLong(0);
    @CountMetric
    public AtomicLong sendSize = new AtomicLong(0);
    @CountMetric
    public AtomicLong sendSuccessCount = new AtomicLong(0);
    @CountMetric
    public AtomicLong sendSuccessSize = new AtomicLong(0);
    @CountMetric
    public AtomicLong sendFailCount = new AtomicLong(0);
    @CountMetric
    public AtomicLong sendFailSize = new AtomicLong(0);
    @CountMetric
    // sinkCallbackTime - sinkBeginTime(milliseconds)
    public AtomicLong sinkDuration = new AtomicLong(0);
    @CountMetric
    // sinkCallbackTime - sourceReceiveTime(milliseconds)
    public AtomicLong nodeDuration = new AtomicLong(0);
    @CountMetric
    // sinkCallbackTime - eventCreateTime(milliseconds)
    public AtomicLong wholeDuration = new AtomicLong(0);

    /**
     * fillInlongId
     * 
     * @param event
     * @param dimensions
     */
    public static void fillInlongId(Event event, Map<String, String> dimensions) {
        Map<String, String> headers = event.getHeaders();
        String inlongGroupId = headers.getOrDefault(Constants.INLONG_GROUP_ID, "");
        String inlongStreamId = headers.getOrDefault(Constants.INLONG_STREAM_ID, "");
        dimensions.put(KEY_INLONG_GROUP_ID, inlongGroupId);
        dimensions.put(KEY_INLONG_STREAM_ID, inlongStreamId);
    }
}
