/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.sink.mqzone;

import static org.apache.inlong.sdk.commons.protocol.EventConstants.HEADER_CACHE_VERSION_1;
import static org.apache.inlong.sdk.commons.protocol.EventConstants.HEADER_KEY_VERSION;
import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.sdk.commons.protocol.EventConstants;

public abstract class AbstractZoneClusterProducer implements LifecycleAware {

    protected final String workerName;
    protected final CacheClusterConfig config;
    protected final AbstractZoneSinkContext sinkContext;
    protected final Context producerContext;
    protected final String cacheClusterName;
    protected LifecycleState state;

    /**
     * Constructor
     *
     * @param workerName
     * @param config
     * @param context
     */
    public AbstractZoneClusterProducer(String workerName, CacheClusterConfig config, AbstractZoneSinkContext context) {
        this.workerName = workerName;
        this.config = config;
        this.sinkContext = context;
        this.producerContext = context.getProducerContext();
        this.state = LifecycleState.IDLE;
        this.cacheClusterName = config.getClusterName();
    }

    /**
     * getLifecycleState
     *
     * @return
     */
    @Override
    public LifecycleState getLifecycleState() {
        return state;
    }

    /**
     * send DispatchProfile
     *
     * @param event DispatchProfile
     * @return boolean sendResult
     */
    public abstract boolean send(DispatchProfile event);

    /**
     * encodeCacheMessageHeaders
     *
     * @param  event
     * @return       Map
     */
    public Map<String, String> encodeCacheMessageHeaders(DispatchProfile event) {
        Map<String, String> headers = new HashMap<>();
        // version int32 protocol version, the value is 1
        headers.put(HEADER_KEY_VERSION, HEADER_CACHE_VERSION_1);
        // inlongGroupId string inlongGroupId
        headers.put(EventConstants.INLONG_GROUP_ID, event.getInlongGroupId());
        // inlongStreamId string inlongStreamId
        headers.put(EventConstants.INLONG_STREAM_ID, event.getInlongStreamId());
        // proxyName string proxy node id, IP or conainer name
        headers.put(EventConstants.HEADER_KEY_PROXY_NAME, sinkContext.getNodeId());
        // packTime int64 pack time, milliseconds
        headers.put(EventConstants.HEADER_KEY_PACK_TIME, String.valueOf(System.currentTimeMillis()));
        // msgCount int32 message count
        headers.put(EventConstants.HEADER_KEY_MSG_COUNT, String.valueOf(event.getEvents().size()));
        // srcLength int32 total length of raw messages body
        headers.put(EventConstants.HEADER_KEY_SRC_LENGTH, String.valueOf(event.getSize()));
        // compressType int
        // compress type of body data
        // INLONG_NO_COMPRESS = 0,
        // INLONG_GZ = 1,
        // INLONG_SNAPPY = 2
        headers.put(EventConstants.HEADER_KEY_COMPRESS_TYPE,
                String.valueOf(sinkContext.getCompressType().getNumber()));
        // messageKey string partition hash key, optional
        return headers;
    }

    /**
     * get cacheClusterName
     *
     * @return the cacheClusterName
     */
    public String getCacheClusterName() {
        return cacheClusterName;
    }
}
