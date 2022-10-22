/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory.TABLE_NAME;
import static io.tidb.bigdata.flink.tidb.TypeUtils.getObjectWithDataType;
import static io.tidb.bigdata.flink.tidb.TypeUtils.toRowDataType;
import static java.lang.String.format;

import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.expression.Expression;
import org.tikv.common.meta.TiTimestamp;

public abstract class TiDBBaseRowDataInputFormat extends
    RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {

  static final Logger LOG = LoggerFactory.getLogger(TiDBBaseRowDataInputFormat.class);

  protected static final String TIMESTAMP_FORMAT_PREFIX = "timestamp-format";

  protected final Map<String, String> properties;

  protected final String databaseName;

  protected final String tableName;

  protected final String[] fieldNames;

  protected final DataType[] fieldTypes;

  protected final TypeInformation<RowData> typeInformation;

  protected final List<SplitInternal> splits;

  protected final List<ColumnHandleInternal> columnHandleInternals;

  protected final TiTimestamp timestamp;

  protected long limit = Long.MAX_VALUE;

  protected long recordCount;

  protected int[] projectedFieldIndexes;

  protected Expression expression;

  protected transient DateTimeFormatter[] formatters;

  protected transient RecordCursorInternal cursor;

  protected transient ClientSession clientSession;

  public TiDBBaseRowDataInputFormat(Map<String, String> properties, String[] fieldNames,
      DataType[] fieldTypes, TypeInformation<RowData> typeInformation) {
    this.properties = Preconditions.checkNotNull(properties, "properties can not be null");
    this.databaseName = getRequiredProperties(DATABASE_NAME.key());
    this.tableName = getRequiredProperties(TABLE_NAME.key());
    this.fieldNames = Arrays.stream(fieldNames).map(String::toLowerCase).toArray(String[]::new);
    this.fieldTypes = fieldTypes;
    this.typeInformation = typeInformation;
    List<ColumnHandleInternal> columns;
    Map<String, Integer> nameAndIndex = new HashMap<>();
    final TiTimestamp splitSnapshotVersion;
    // get split
    try (ClientSession splitSession = ClientSession
        .create(new ClientConfig(properties))) {
      // check exist
      splitSession.getTableMust(databaseName, tableName);
      TableHandleInternal tableHandleInternal = new TableHandleInternal(
          UUID.randomUUID().toString(), this.databaseName, this.tableName);
      SplitManagerInternal splitManagerInternal = new SplitManagerInternal(splitSession);
      this.splits = splitManagerInternal.getSplits(tableHandleInternal);
      columns = splitSession.getTableColumns(tableHandleInternal)
          .orElseThrow(() -> new NullPointerException("columnHandleInternals is null"));
      IntStream.range(0, columns.size())
          .forEach(i -> nameAndIndex.put(columns.get(i).getName(), i));
      splitSnapshotVersion = splitSession.getSnapshotVersion();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    // check flink table column names
    Arrays.stream(this.fieldNames)
        .forEach(name -> Preconditions.checkState(nameAndIndex.containsKey(name),
            format("can not find column: %s in table `%s`.`%s`", name, databaseName, tableName)));
    // We should filter columns, because the number of tidb columns may greater than flink columns
    columnHandleInternals = Arrays.stream(this.fieldNames)
        .map(name -> columns.get(nameAndIndex.get(name))).collect(Collectors.toList());
    projectedFieldIndexes = IntStream.range(0, this.fieldNames.length).toArray();
    timestamp = getOptionalVersion()
        .orElseGet(() -> getOptionalTimestamp().orElse(splitSnapshotVersion));
  }

  private Optional<TiTimestamp> getOptionalTimestamp() {
    return Optional
        .ofNullable(properties.get(ClientConfig.SNAPSHOT_TIMESTAMP))
        .filter(StringUtils::isNoneEmpty)
        .map(s -> new TiTimestamp(Timestamp.from(ZonedDateTime.parse(s).toInstant()).getTime(), 0));
  }

  private Optional<TiTimestamp> getOptionalVersion() {
    return Optional
        .ofNullable(properties.get(ClientConfig.SNAPSHOT_VERSION))
        .filter(StringUtils::isNoneEmpty)
        .map(Long::parseUnsignedLong)
        .map(tso -> new TiTimestamp(tso >> 18, tso & 0x3FFFF));
  }

  public long getSnapshotVersion() {
    return timestamp.getVersion();
  }

  @Override
  public void configure(Configuration parameters) {
    // do nothing here
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
    return cachedStatistics;
  }

  @Override
  public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
    GenericInputSplit[] inputSplits = new GenericInputSplit[splits.size()];
    for (int i = 0; i < inputSplits.length; i++) {
      inputSplits[i] = new GenericInputSplit(i, inputSplits.length);
    }
    return inputSplits;
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void openInputFormat() throws IOException {
    formatters = TypeUtils.extractDateTimeFormatter(fieldNames, properties, true);
    clientSession = ClientSession.create(new ClientConfig(properties));
  }

  @Override
  public void closeInputFormat() throws IOException {
    if (clientSession != null) {
      try {
        clientSession.close();
      } catch (Exception e) {
        LOG.warn("can not close clientSession", e);
      }
    }
  }

  @Override
  public void open(InputSplit split) throws IOException {
    if (recordCount >= limit) {
      return;
    }
    SplitInternal splitInternal = splits.get(split.getSplitNumber());
    RecordSetInternal recordSetInternal = new RecordSetInternal(clientSession, splitInternal,
        Arrays.stream(projectedFieldIndexes).mapToObj(columnHandleInternals::get)
            .collect(Collectors.toList()),
        Optional.ofNullable(expression),
        Optional.ofNullable(timestamp),
        limit > Integer.MAX_VALUE ? Optional.empty() : Optional.of((int) limit));
    cursor = recordSetInternal.cursor();
  }

  @Override
  public void close() throws IOException {
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return recordCount >= limit || !cursor.advanceNextPosition();
  }

  public GenericRowData nextRecordWithFactory(Function<Integer, GenericRowData> rowDataFactory)
      throws IOException {
    GenericRowData row = rowDataFactory.apply(projectedFieldIndexes.length);
    for (int i = 0; i < projectedFieldIndexes.length; i++) {
      int projectedFieldIndex = projectedFieldIndexes[i];
      DataType fieldType = fieldTypes[projectedFieldIndex];
      Object object = cursor.getObject(i);
      // data can be null here
      row.setField(i, toRowDataType(
          getObjectWithDataType(object, fieldType,
              columnHandleInternals.get(projectedFieldIndex).getType(),
              formatters[projectedFieldIndex]).orElse(null)));
    }
    recordCount++;
    return row;
  }

  @Override
  public RowData nextRecord(RowData rowData) throws IOException {
    return nextRecordWithFactory(GenericRowData::new);
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return typeInformation;
  }

  protected String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(properties.get(key), key + " can not be null");
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public void setProjectedFields(int[][] projectedFields) {
    this.projectedFieldIndexes = new int[projectedFields.length];
    for (int i = 0; i < projectedFields.length; i++) {
      int[] projectedField = projectedFields[i];
      // not support nested projection
      Preconditions.checkArgument(projectedField != null && projectedField.length == 1,
          "projected field can not be null and length must be 1");
      this.projectedFieldIndexes[i] = projectedField[0];
    }
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }
}
