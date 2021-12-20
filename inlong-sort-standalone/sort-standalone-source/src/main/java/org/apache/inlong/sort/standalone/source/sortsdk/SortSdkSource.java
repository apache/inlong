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

package org.apache.inlong.sort.standalone.source.sortsdk;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.inlong.sdk.sort.api.SortClient;
import org.apache.inlong.sdk.sort.api.SortClientConfig;
import org.apache.inlong.sdk.sort.api.SortClientFactory;
import org.apache.inlong.sdk.sort.entity.MessageRecord;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.SortTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default Source implementation of InLong.
 *
 * <p> SortSdkSource acquired msg from different upstream data store by register {@link SortClient} for each
 * sort task. The only things SortSdkSource should do is to get one client by the sort task id, or remove one client
 * when the task is finished or schedule to other source instance. </p>
 *
 * <p> The Default Manager of InLong will schedule the partition and topic automatically. </p>
 *
 * <p> Because all sources should implement {@link Configurable}, the SortSdkSource should have
 * default constructor <b>WITHOUT</b> any arguments, and parameters will be configured by
 * {@link Configurable#configure(Context)}. </p>
 */
final public class SortSdkSource extends AbstractSource implements Configurable, Runnable, EventDrivenSource {

    /** Log of {@link SortSdkSource}. */
    private static final Logger LOG = LoggerFactory.getLogger(SortSdkSource.class);

    /** Default pool of {@link ScheduledExecutorService}. */
    private static final int CORE_POOL_SIZE = 1;

    /** Default consume strategy of {@link SortClient}. */
    private static final  SortClientConfig.ConsumeStrategy defaultStrategy = SortClientConfig.ConsumeStrategy.lastest;

    /** Map of {@link SortClient}. */
    private ConcurrentHashMap<String, SortClient> clients;

    /** The cluster name of sort. */
    private String sortClusterName;

    /** Reload config interval. */
    private long reloadInterval;

    /** Context of SortSdkSource. */
    private SortSdkSourceContext context;

    /** Executor for config reloading. */
    private ScheduledExecutorService pool;

    /**
     * Start SortSdkSource.
     */
    @Override
    public synchronized void start() {
        this.reload();
    }

    /**
     * Stop {@link #pool} and close all {@link SortClient}.
     */
    @Override
    public void stop() {
        pool.shutdownNow();
        clients.forEach((sortId, client) -> client.close());
    }

    /**
     * Entrance of {@link #pool} to reload clients with fix rate {@link #reloadInterval}.
     */
    @Override
    public void run() {
        this.reload();
    }

    /**
     * Configure parameters.
     *
     * @param context Context of source.
     */
    @Override
    public void configure(Context context) {
        this.clients = new ConcurrentHashMap<>();
        this.sortClusterName = SortClusterConfigHolder.getClusterConfig().getClusterName();
        Preconditions.checkState(context != null, "No context, configure failed");
        this.context = new SortSdkSourceContext(getName(), context);
        this.reloadInterval = this.context.getReloadInterval();
        this.initReloadExecutor();
    }

    /**
     * Init ScheduledExecutorService with fix reload rate {@link #reloadInterval}.
     */
    private void initReloadExecutor() {
        this.pool = Executors.newScheduledThreadPool(CORE_POOL_SIZE);
        pool.scheduleAtFixedRate(this, reloadInterval, reloadInterval, TimeUnit.SECONDS);
    }

    /**
     * Reload clients by current {@link SortTaskConfig}.
     *
     * <p> Create new clients with new sort task id, and remove the finished or scheduled ones. </p>
     *
     * <p> Current version of SortSdk <b>DO NOT</b> support to get the corresponding sort id of {@link SortClient}.
     *  Hence, the maintenance of mapping of <SortId, SortClient> should be done by Source itself. Which is not elegant,
     *  the <b>REMOVE</b> of expire clients will <b>NOT</b> be supported right now. </p>
     */
    private void reload() {

        final List<SortTaskConfig> configs = SortClusterConfigHolder.getClusterConfig().getSortTasks();
        LOG.info("start to reload SortSdkSource");

        // Start new clients
        for (SortTaskConfig taskConfig : configs) {

            // If exits, skip.
            final String sortId = taskConfig.getName();
            SortClient client = this.clients.get(sortId);
            if (client != null) {
                continue;
            }

            // Otherwise, new one client.
            client = this.newClient(sortId);
            if (client != null) {
                this.clients.put(sortId, client);
            }
        }
    }

    /**
     * Create one {@link SortClient} with specific sort id.
     *
     * <p> In current version, the {@link FetchCallback} will hold the client to ACK.
     * For more details see {@link FetchCallback#onFinished(MessageRecord)}</p>
     *
     * @param sortId Sort in of new client.
     *
     * @return New sort client.
     */
    private SortClient newClient(String sortId) {
        LOG.info("Start to new sort client for id: {}", sortId);
        try {
            final SortClientConfig clientConfig =
                    new SortClientConfig(sortId, this.sortClusterName, new DefaultTopicChangeListener(),
                            SortSdkSource.defaultStrategy, InetAddress.getLocalHost().getHostAddress());
            final FetchCallback callback = FetchCallback.Factory.create(sortId, getChannelProcessor(), context);
            clientConfig.setCallback(callback);
            SortClient client = SortClientFactory.createSortClient(clientConfig);
            client.init();
            // temporary use to ACK fetched msg.
            callback.setClient(client);
            return client;
        } catch (UnknownHostException ex) {
            LOG.error("Got one UnknownHostException when init client of id: " + sortId, ex);
        } catch (Throwable th) {
            LOG.error("Got one throwable when init client of id: " + sortId, th);
        }
        return null;
    }

}
