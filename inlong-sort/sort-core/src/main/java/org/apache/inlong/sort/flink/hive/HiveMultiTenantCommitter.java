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

package org.apache.inlong.sort.flink.hive;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.flink.hive.partition.JdbcHivePartitionCommitPolicy;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitInfo;
import org.apache.inlong.sort.flink.hive.partition.PartitionCommitPolicy;
import org.apache.inlong.sort.meta.MetaManager;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("SynchronizeOnNonFinalField")
public class HiveMultiTenantCommitter extends ProcessFunction<PartitionCommitInfo, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMultiTenantCommitter.class);

    private static final long serialVersionUID = -8084389212282039905L;

    private final Configuration configuration;

    private final PartitionCommitPolicy.Factory commitPolicyFactory;

    private transient Map<Long, HiveCommitter> committers;

    private transient SinkContextProxy contextProxy;

    public HiveMultiTenantCommitter(Configuration configuration) {
        this(configuration, new JdbcHivePartitionCommitPolicy.Factory());
    }

    public HiveMultiTenantCommitter(Configuration configuration, PartitionCommitPolicy.Factory commitPolicyFactory) {
        this.configuration = Preconditions.checkNotNull(configuration);
        this.commitPolicyFactory = Preconditions.checkNotNull(commitPolicyFactory);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        committers = new HashMap<>();
        MetaManager metaManager = MetaManager.getInstance(configuration);
        metaManager.registerDataFlowInfoListener(new DataFlowInfoListenerImpl());
        contextProxy = new SinkContextProxy();
    }

    @Override
    public void close() throws Exception {
        MetaManager.release();
        synchronized (committers) {
            for (HiveCommitter committer: committers.values()) {
                committer.close();
            }
            committers.clear();
        }
    }

    @Override
    public void processElement(PartitionCommitInfo commitInfo, Context context, Collector<Void> collector)
            throws Exception {
        final long dataFlowId = commitInfo.getDataFlowId();

        synchronized (committers) {
            final HiveCommitter committer = committers.get(dataFlowId);
            if (committer == null) {
                LOG.warn("Cannot get DataFlowInfo with id {}", dataFlowId);
                return;
            }

            committer.invoke(commitInfo, contextProxy.setContext(context));
        }
    }

    private static class SinkContextProxy implements SinkFunction.Context {

        private Context context;

        private SinkContextProxy setContext(Context context) {
            this.context = context;
            return this;
        }

        @Override
        public long currentProcessingTime() {
            return context.timerService().currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return context.timerService().currentWatermark();
        }

        @Override
        public Long timestamp() {
            return context.timestamp();
        }
    }

    private class DataFlowInfoListenerImpl implements DataFlowInfoListener {

        @Override
        public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (committers) {
                long dataFlowId = dataFlowInfo.getId();
                SinkInfo sinkInfo = dataFlowInfo.getSinkInfo();
                if (!(sinkInfo instanceof HiveSinkInfo)) {
                    LOG.error("SinkInfo type {} of dataFlow {} doesn't match application sink type 'hive'!",
                            sinkInfo.getClass(), dataFlowId);
                    return;
                }
                HiveSinkInfo hiveSinkInfo = (HiveSinkInfo) sinkInfo;
                HiveCommitter hiveCommitter = new HiveCommitter(configuration, hiveSinkInfo, commitPolicyFactory);
                hiveCommitter.setRuntimeContext(getRuntimeContext());
                hiveCommitter.open(new org.apache.flink.configuration.Configuration());
                committers.put(dataFlowId, hiveCommitter);
            }
        }

        @Override
        public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (committers) {
                removeDataFlow(dataFlowInfo);
                addDataFlow(dataFlowInfo);
            }
        }

        @Override
        public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
            synchronized (committers) {
                final HiveCommitter existingHiveCommitter = committers.remove(dataFlowInfo.getId());
                if (existingHiveCommitter != null) {
                    existingHiveCommitter.close();
                }
            }
        }

    }
}
