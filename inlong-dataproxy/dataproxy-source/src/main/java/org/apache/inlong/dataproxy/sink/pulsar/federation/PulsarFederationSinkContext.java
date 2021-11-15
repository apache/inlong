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

package org.apache.inlong.dataproxy.sink.pulsar.federation;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.inlong.commons.config.metrics.MetricRegister;
import org.apache.inlong.dataproxy.config.RemoteConfigManager;
import org.apache.inlong.dataproxy.config.holder.CacheClusterConfigHolder;
import org.apache.inlong.dataproxy.config.holder.CommonPropertiesHolder;
import org.apache.inlong.dataproxy.config.holder.IdTopicConfigHolder;
import org.apache.inlong.dataproxy.utils.BufferQueue;

/**
 * 
 * PulsarFederationContext
 */
public class PulsarFederationSinkContext {

    private final String proxyClusterId;
    private final Context sinkContext;
    private final Context producerContext;
    //
    private final IdTopicConfigHolder idTopicHolder;
    private final CacheClusterConfigHolder cacheHolder;
    private final BufferQueue<Event> bufferQueue;
    //
    private final int maxThreads;
    private final int maxTransaction;
    private final long processInterval;
    private final long reloadInterval;
    //
    private final PulsarFederationSinkMetricItemSet metricItemSet;

    /**
     * Constructor
     * 
     * @param context
     */
    public PulsarFederationSinkContext(Context context) {
        this.proxyClusterId = CommonPropertiesHolder.getString(RemoteConfigManager.KEY_PROXY_CLUSTER_NAME, "unknown");
        this.sinkContext = context;
        this.maxThreads = context.getInteger("max-threads", 10);
        this.maxTransaction = context.getInteger("maxTransaction", 1);
        this.processInterval = context.getInteger("processInterval", 100);
        this.reloadInterval = context.getLong("reloadInterval", 60000L);
        //
        this.idTopicHolder = new IdTopicConfigHolder();
        this.idTopicHolder.configure(context);
        this.idTopicHolder.start();
        //
        this.cacheHolder = new CacheClusterConfigHolder();
        this.cacheHolder.configure(context);
        this.cacheHolder.start();
        //
        int maxBufferQueueSize = context.getInteger("maxBufferQueueSize", 128 * 1024);
        this.bufferQueue = new BufferQueue<Event>(maxBufferQueueSize);
        //
        Map<String, String> producerParams = context.getSubProperties("producer.");
        this.producerContext = new Context(producerParams);
        //
        this.metricItemSet = new PulsarFederationSinkMetricItemSet();
        MetricRegister.register(this.metricItemSet);
    }

    /**
     * close
     */
    public void close() {
        this.idTopicHolder.close();
        this.cacheHolder.close();
    }

    /**
     * get proxyClusterId
     * 
     * @return the proxyClusterId
     */
    public String getProxyClusterId() {
        return proxyClusterId;
    }

    /**
     * get sinkContext
     * 
     * @return the sinkContext
     */
    public Context getSinkContext() {
        return sinkContext;
    }

    /**
     * get producerContext
     * 
     * @return the producerContext
     */
    public Context getProducerContext() {
        return producerContext;
    }

    /**
     * get idTopicHolder
     * 
     * @return the idTopicHolder
     */
    public IdTopicConfigHolder getIdTopicHolder() {
        return idTopicHolder;
    }

    /**
     * get cacheHolder
     * 
     * @return the cacheHolder
     */
    public CacheClusterConfigHolder getCacheHolder() {
        return cacheHolder;
    }

    /**
     * get bufferQueue
     * 
     * @return the bufferQueue
     */
    public BufferQueue<Event> getBufferQueue() {
        return bufferQueue;
    }

    /**
     * get maxThreads
     * 
     * @return the maxThreads
     */
    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * get maxTransaction
     * 
     * @return the maxTransaction
     */
    public int getMaxTransaction() {
        return maxTransaction;
    }

    /**
     * get processInterval
     * 
     * @return the processInterval
     */
    public long getProcessInterval() {
        return processInterval;
    }

    /**
     * get reloadInterval
     * 
     * @return the reloadInterval
     */
    public long getReloadInterval() {
        return reloadInterval;
    }

    /**
     * get metricItemSet
     * 
     * @return the metricItemSet
     */
    public PulsarFederationSinkMetricItemSet getMetricItemSet() {
        return metricItemSet;
    }

}
