/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.source.reader;

import io.tidb.bigdata.flink.connector.source.TiDBSchemaAdapter;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.ColumnHandleInternal;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import io.tidb.bigdata.tidb.RecordSetInternal;
import io.tidb.bigdata.tidb.SplitInternal;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.tikv.common.expression.Expression;
import org.tikv.common.meta.TiTimestamp;

public class TiDBSourceSplitRecords implements RecordsWithSplitIds<RowData> {

  private final Set<String> finishedSplits;
  private final TiDBSourceSplit[] splits;
  private int nextSplit;
  private final ClientSession session;
  private RecordCursorInternal cursor;
  private final List<ColumnHandleInternal> columns;
  private final TiDBSchemaAdapter schema;
  private final TiTimestamp timestamp;
  private final Expression expression;
  private final Integer limit;

  public TiDBSourceSplitRecords(ClientSession session, List<TiDBSourceSplit> splits,
      List<ColumnHandleInternal> columns, TiDBSchemaAdapter schema, Expression expression,
      Integer limit, TiTimestamp timestamp) {
    this.session = session;
    this.splits = splits.toArray(new TiDBSourceSplit[0]);
    this.finishedSplits = splits.stream().map(TiDBSourceSplit::splitId).collect(Collectors.toSet());
    this.schema = schema;
    this.columns = columns;
    this.timestamp = Optional.ofNullable(timestamp)
        .orElseGet(() -> this.splits[0].getSplit().getTimestamp());
    this.expression = expression;
    this.limit = limit;
  }

  @Nullable
  @Override
  public String nextSplit() {
    if (nextSplit >= splits.length) {
      return null;
    }
    int currentSplit = nextSplit;
    nextSplit = currentSplit + 1;
    TiDBSourceSplit split = splits[currentSplit];
    SplitInternal splitInternal = split.getSplit();
    RecordSetInternal recordSetInternal = new RecordSetInternal(session,
        splitInternal, columns, Optional.ofNullable(expression), Optional.of(timestamp),
        Optional.ofNullable(limit));
    cursor = recordSetInternal.cursor();
    return splits[currentSplit].splitId();
  }

  @Nullable
  @Override
  public RowData nextRecordFromSplit() {
    if (!cursor.advanceNextPosition()) {
      return null;
    }

    return schema.convert(timestamp, cursor);
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }
}