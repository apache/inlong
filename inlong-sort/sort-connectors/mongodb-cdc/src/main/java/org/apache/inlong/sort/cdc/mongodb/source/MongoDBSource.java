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

package org.apache.inlong.sort.cdc.mongodb.source;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Supplier;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.metrics.MetricGroup;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.cdc.base.config.MetricConfig;
import org.apache.inlong.sort.cdc.base.config.SourceConfig;
import org.apache.inlong.sort.cdc.base.debezium.DebeziumDeserializationSchema;
import org.apache.inlong.sort.cdc.base.source.IncrementalSource;
import org.apache.inlong.sort.cdc.base.source.meta.split.SourceRecords;
import org.apache.inlong.sort.cdc.base.source.meta.split.SourceSplitState;
import org.apache.inlong.sort.cdc.base.source.metrics.SourceReaderMetrics;
import org.apache.inlong.sort.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.inlong.sort.cdc.base.source.reader.IncrementalSourceSplitReader;
import org.apache.inlong.sort.cdc.mongodb.source.config.MongoDBSourceConfig;
import org.apache.inlong.sort.cdc.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.inlong.sort.cdc.mongodb.source.dialect.MongoDBDialect;
import org.apache.inlong.sort.cdc.mongodb.source.offset.ChangeStreamOffsetFactory;
import org.apache.inlong.sort.cdc.mongodb.source.reader.MongoDBRecordEmitter;

/**
 * The MongoDB CDC Source based on FLIP-27 which supports parallel reading snapshot of collection
 * and then continue to capture data change from change stream.
 *
 * <pre>
 *     1. The source supports parallel capturing database(s) or collection(s) change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of MongoDB.
 * </pre>
 *
 * <pre>{@code
 * MongoDBSource
 *     .<String>builder()
 *     .hosts("localhost:27017")
 *     .databaseList("mydb")
 *     .collectionList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MongoDBSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 * Copy from com.ververica:flink-connector-mongodb-cdc:2.3.0.
 */
@Internal
@Experimental
public class MongoDBSource<T> extends IncrementalSource<T, MongoDBSourceConfig> {

    private static final long serialVersionUID = 1L;

    MongoDBSource(
            MongoDBSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        super(
                configFactory,
                deserializationSchema,
                new ChangeStreamOffsetFactory(),
                new MongoDBDialect());
    }

    @Override
    public IncrementalSourceReader<T, MongoDBSourceConfig> createReader(SourceReaderContext readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        MongoDBSourceConfig sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        MetricConfig metricConfig = (MetricConfig) sourceConfig;
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        // Forward compatible with flink 1.13
        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);
        final SourceReaderMetrics sourceReaderMetrics = new SourceReaderMetrics(metricGroup);

        // create source config for the given subtask (e.g. unique server id)
        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(metricConfig.getInlongMetric())
                .withAuditAddress(metricConfig.getInlongAudit())
                .withRegisterMetric(RegisteredMetric.ALL)
                .build();

        sourceReaderMetrics.registerMetrics(metricOption,
                Arrays.asList(Constants.DATABASE_NAME, Constants.COLLECTION_NAME));
        Supplier<IncrementalSourceSplitReader<MongoDBSourceConfig>> splitReaderSupplier =
                () -> new IncrementalSourceSplitReader<>(
                        readerContext.getIndexOfSubtask(), dataSourceDialect, sourceConfig);
        return new IncrementalSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                createRecordEmitter(sourceConfig, sourceReaderMetrics),
                readerContext.getConfiguration(),
                readerContext,
                sourceConfig,
                sourceSplitSerializer,
                dataSourceDialect,
                sourceReaderMetrics);
    }

    /**
     * Get a MongoDBSourceBuilder to build a {@link MongoDBSource}.
     *
     * @return a MongoDB parallel source builder.
     */
    @PublicEvolving
    public static <T> MongoDBSourceBuilder<T> builder() {
        return new MongoDBSourceBuilder<>();
    }

    @Override
    protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        return new MongoDBRecordEmitter<>(
                deserializationSchema, sourceReaderMetrics, offsetFactory);
    }
}
