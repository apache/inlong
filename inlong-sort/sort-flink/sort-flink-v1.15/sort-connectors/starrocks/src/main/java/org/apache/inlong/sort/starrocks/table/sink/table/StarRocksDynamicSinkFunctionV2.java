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

package org.apache.inlong.sort.starrocks.table.sink.table;

import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.SinkExactlyMetric;
import org.apache.inlong.sort.starrocks.table.sink.utils.SchemaUtils;

import com.google.common.base.Strings;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManagerV2;
import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionBase;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.table.sink.StarRocksVersionedSerializer;
import com.starrocks.connector.flink.table.sink.StarrocksSnapshotState;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.inlong.sort.base.util.CalculateObjectSizeUtils.getDataSize;

/**
 * StarRocks dynamic sink function. It supports insert, upsert, delete in Starrocks.
 * @param <T>
 */
public class StarRocksDynamicSinkFunctionV2<T> extends StarRocksDynamicSinkFunctionBase<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(StarRocksDynamicSinkFunctionV2.class);

    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksSinkManagerV2 sinkManager;
    private final StarRocksISerializer serializer;
    private final StarRocksIRowTransformer<T> rowTransformer;
    private static final int NESTED_ROW_DATA_HEADER_SIZE = 256;

    private transient volatile ListState<StarrocksSnapshotState> snapshotStates;
    private final Map<Long, List<StreamLoadSnapshot>> snapshotMap = new ConcurrentHashMap<>();
    private transient SinkExactlyMetric sinkExactlyMetric;

    @Deprecated
    private transient ListState<Map<String, StarRocksSinkBufferEntity>> legacyState;
    @Deprecated
    private transient List<StarRocksSinkBufferEntity> legacyData;
    private String inlongMetric;
    private String auditHostAndPorts;
    private String auditKeys;
    private SchemaUtils schemaUtils;
    private String stateKey;

    public StarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions,
            TableSchema schema,
            StarRocksIRowTransformer<T> rowTransformer, String inlongMetric,
            String auditHostAndPorts, String auditKeys) {
        this.sinkOptions = sinkOptions;
        this.rowTransformer = rowTransformer;
        this.auditHostAndPorts = auditHostAndPorts;
        this.inlongMetric = inlongMetric;
        this.auditKeys = auditKeys;
        StarRocksSinkTable sinkTable = StarRocksSinkTable.builder()
                .sinkOptions(sinkOptions)
                .build();
        this.schemaUtils = new SchemaUtils(schema);
        // StarRocksJsonSerializer depends on SinkOptions#supportUpsertDelete which is decided in
        // StarRocksSinkTable#validateTableStructure, so create serializer after validating table structure
        this.serializer = StarRocksSerializerFactory.createSerializer(sinkOptions,
                schemaUtils.filterOutTimeField(schema));
        rowTransformer.setStarRocksColumns(sinkTable.getFieldMapping());
        rowTransformer.setTableSchema(schema);
        this.sinkManager = new StarRocksSinkManagerV2(sinkOptions.getProperties(),
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
    }

    public StarRocksDynamicSinkFunctionV2(
            StarRocksSinkOptions sinkOptions,
            TableSchema schema,
            StarRocksIRowTransformer<T> rowTransformer, String inlongMetric,
            String auditHostAndPorts, String auditKeys, String stateKey) {
        this(sinkOptions, schema, rowTransformer, inlongMetric, auditHostAndPorts, auditKeys);
        this.stateKey = stateKey;
    }

    @Override
    public void invoke(T value, Context context)
            throws IOException, ClassNotFoundException, JSQLParserException {

        if (serializer == null) {
            if (value instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getDataRows() == null) {
                    log.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), Arrays.toString(data.getDataRows())));
                    return;
                }
                sinkManager.write(null, data.getDatabase(), data.getTable(), data.getDataRows());
                return;
            } else if (value instanceof StarRocksRowData) {
                StarRocksRowData data = (StarRocksRowData) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getRow() == null) {
                    log.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), data.getRow()));
                    return;
                }
                sinkManager.write(data.getUniqueKey(), data.getDatabase(), data.getTable(), data.getRow());
                return;
            }
            // raw data sink
            sinkManager.write(null, sinkOptions.getDatabaseName(), sinkOptions.getTableName(), value.toString());
            return;
        }

        if (value instanceof NestedRowData) {
            NestedRowData ddlData = (NestedRowData) value;
            if (ddlData.getSegments().length != 1 || ddlData.getSegments()[0].size() < NESTED_ROW_DATA_HEADER_SIZE) {
                return;
            }

            int totalSize = ddlData.getSegments()[0].size();
            byte[] data = new byte[totalSize - NESTED_ROW_DATA_HEADER_SIZE];
            ddlData.getSegments()[0].get(NESTED_ROW_DATA_HEADER_SIZE, data);
            Map<String, String> ddlMap = InstantiationUtil.deserializeObject(data, HashMap.class.getClassLoader());
            if (ddlMap == null
                    || "true".equals(ddlMap.get("snapshot"))
                    || Strings.isNullOrEmpty(ddlMap.get("ddl"))
                    || Strings.isNullOrEmpty(ddlMap.get("databaseName"))) {
                return;
            }
            Statement statement = CCJSqlParserUtil.parse(ddlMap.get("ddl"));
            if (statement instanceof Truncate) {
                Truncate truncate = (Truncate) statement;
                if (!sinkOptions.getTableName().equalsIgnoreCase(truncate.getTable().getName())) {
                    return;
                }
                // TODO: add ddl to queue
            } else if (statement instanceof Alter) {

            }
        }
        if (value instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData) value).getRowKind())) {
                // do not need update_before, cause an update action happened on the primary keys will be separated into
                // `delete` and `create`
                return;
            }
            if (!sinkOptions.supportUpsertDelete() && RowKind.DELETE.equals(((RowData) value).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
                return;
            }
        }

        flushLegacyData();

        Object[] data = rowTransformer.transform(value, sinkOptions.supportUpsertDelete());

        sinkManager.write(
                null,
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializer.serialize(schemaUtils.filterOutTimeField(data)));

        ouputMetrics(value, data);
    }

    private void ouputMetrics(T value, Object[] data) {
        if (sinkExactlyMetric != null) {
            sinkExactlyMetric.invoke(1, getDataSize(value), schemaUtils.getDataTime(data));
        }
    }

    @Override
    public void open(Configuration parameters) {
        sinkManager.init();
        sinkManager.setRuntimeContext(getRuntimeContext(), sinkOptions);
        if (rowTransformer != null) {
            rowTransformer.setRuntimeContext(getRuntimeContext());
        }

        MetricOption metricOption = MetricOption.builder().withInlongLabels(inlongMetric)
                .withAuditAddress(auditHostAndPorts)
                .withAuditKeys(auditKeys)
                .build();

        if (metricOption != null) {
            sinkExactlyMetric = new SinkExactlyMetric(metricOption, getRuntimeContext().getMetricGroup());
        }

        commitTransaction(Long.MAX_VALUE);
        log.info("Open sink function v2. {}", EnvUtils.getGitInformation());
    }

    @Override
    public void finish() {
        sinkManager.flush();
    }

    @Override
    public void close() {
        try {
            sinkManager.flush();
        } catch (Exception e) {
            log.error("Failed to flush when closing", e);
            throw e;
        } finally {
            StreamLoadSnapshot snapshot = sinkManager.snapshot();
            sinkManager.abort(snapshot);
            sinkManager.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        updateCurrentCheckpointId(functionSnapshotContext.getCheckpointId());
        sinkManager.flush();

        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        StreamLoadSnapshot snapshot = sinkManager.snapshot();

        if (sinkManager.prepare(snapshot)) {
            snapshotMap.put(functionSnapshotContext.getCheckpointId(), Collections.singletonList(snapshot));

            snapshotStates.clear();
            snapshotStates.add(StarrocksSnapshotState.of(snapshotMap));
        } else {
            sinkManager.abort(snapshot);
            throw new RuntimeException("Snapshot state failed by prepare");
        }

        if (legacyState != null) {
            legacyState.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        String transactionStateName = "starrocks-sink-transaction"
                + (StringUtils.isNullOrWhitespaceOnly(stateKey) ? "" : "-" + stateKey);
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>(
                        transactionStateName,
                        TypeInformation.of(new TypeHint<byte[]>() {
                        }));

        ListState<byte[]> listState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        snapshotStates = new SimpleVersionedListState<>(listState, new StarRocksVersionedSerializer());

        String legacyStateName = "buffered-rows"
                + (StringUtils.isNullOrWhitespaceOnly(stateKey) ? "" : "-" + stateKey);
        // old version
        ListStateDescriptor<Map<String, StarRocksSinkBufferEntity>> legacyDescriptor =
                new ListStateDescriptor<>(
                        legacyStateName,
                        TypeInformation.of(new TypeHint<Map<String, StarRocksSinkBufferEntity>>() {
                        }));
        legacyState = functionInitializationContext.getOperatorStateStore().getListState(legacyDescriptor);

        if (functionInitializationContext.isRestored()) {
            for (StarrocksSnapshotState state : snapshotStates.get()) {
                for (Map.Entry<Long, List<StreamLoadSnapshot>> entry : state.getData().entrySet()) {
                    snapshotMap.compute(entry.getKey(), (k, v) -> {
                        if (v == null) {
                            return new ArrayList<>(entry.getValue());
                        }
                        v.addAll(entry.getValue());
                        return v;
                    });
                }
            }

            legacyData = new ArrayList<>();
            for (Map<String, StarRocksSinkBufferEntity> entry : legacyState.get()) {
                legacyData.addAll(entry.values());
            }
            log.info("There are {} items from legacy state", legacyData.size());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        commitTransaction(checkpointId);
        flushAudit();
        updateLastCheckpointId(checkpointId);
    }

    private void commitTransaction(long checkpointId) {
        boolean succeed = true;

        List<Long> commitCheckpointIds = snapshotMap.keySet().stream()
                .filter(cpId -> cpId <= checkpointId)
                .sorted(Long::compare)
                .collect(Collectors.toList());

        for (Long cpId : commitCheckpointIds) {
            try {
                for (StreamLoadSnapshot snapshot : snapshotMap.get(cpId)) {
                    if (!sinkManager.commit(snapshot)) {
                        succeed = false;
                        break;
                    }
                }

                if (!succeed) {
                    throw new RuntimeException(String.format("Failed to commit some transactions for snapshot %s, " +
                            "please check taskmanager logs for details", cpId));
                }
            } catch (Exception e) {
                log.error("Failed to notify checkpoint complete, checkpoint id : {}", checkpointId, e);
                throw new RuntimeException("Failed to notify checkpoint complete for checkpoint id " + checkpointId, e);
            }

            snapshotMap.remove(cpId);
        }

        // set legacyState to null to avoid clear it in latter snapshotState
        legacyState = null;
    }

    private void flushLegacyData() {
        if (legacyData == null || legacyData.isEmpty()) {
            return;
        }

        for (StarRocksSinkBufferEntity entity : legacyData) {
            for (byte[] data : entity.getBuffer()) {
                sinkManager.write(null, entity.getDatabase(), entity.getTable(),
                        new String(data, StandardCharsets.UTF_8));
            }
            log.info("Write {} legacy records from table '{}' of database '{}'",
                    entity.getBuffer().size(), entity.getDatabase(), entity.getTable());
        }
        legacyData.clear();
    }

    private void flushAudit() {
        if (sinkExactlyMetric != null) {
            sinkExactlyMetric.flushAudit();
        }
    }

    private void updateCurrentCheckpointId(long checkpointId) {
        if (sinkExactlyMetric != null) {
            sinkExactlyMetric.updateCurrentCheckpointId(checkpointId);
        }
    }

    private void updateLastCheckpointId(long checkpointId) {
        if (sinkExactlyMetric != null) {
            sinkExactlyMetric.updateLastCheckpointId(checkpointId);
        }
    }

}
