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
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.tikv.common.expression.Expression;
import org.tikv.common.meta.TiTimestamp;

public class TiDBSourceSplitReader implements SplitReader<RowData, TiDBSourceSplit> {

  private final ClientSession session;
  private final List<ColumnHandleInternal> columns;
  private final TiDBSchemaAdapter schema;
  private final Expression expression;
  private final Integer limit;
  private final TiTimestamp timestamp;

  private List<TiDBSourceSplit> splits;
  private static final List<TiDBSourceSplit> EMPTY_SPLITS = new ArrayList<>(0);

  public TiDBSourceSplitReader(ClientSession session, List<ColumnHandleInternal> columns,
      TiDBSchemaAdapter schema, Expression expression, Integer limit, TiTimestamp timestamp) {
    this.session = session;
    this.columns = columns;
    this.schema = schema;
    this.expression = expression;
    this.limit = limit;
    this.timestamp = timestamp;
  }

  @Override
  public RecordsWithSplitIds<RowData> fetch() {
    try {
      return new TiDBSourceSplitRecords(session, splits, columns, schema, expression, limit,
          timestamp);
    } finally {
      splits = EMPTY_SPLITS;
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<TiDBSourceSplit> splitsChange) {
    // Get all the partition assignments and stopping offsets.
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          String.format(
              "The SplitChange type of %s is not supported.",
              splitsChange.getClass()));
    }
    splits = splitsChange.splits();
  }

  @Override
  public void wakeUp() {
  }

  @Override
  public void close() throws Exception {
    session.close();
  }
}
