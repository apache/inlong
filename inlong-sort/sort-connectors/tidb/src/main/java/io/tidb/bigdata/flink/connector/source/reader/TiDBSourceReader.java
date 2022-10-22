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

import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplitState;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;

public class TiDBSourceReader extends
    SingleThreadMultiplexSourceReaderBase<RowData, RowData,
        TiDBSourceSplit, TiDBSourceSplitState> {

  public TiDBSourceReader(
      Supplier<SplitReader<RowData, TiDBSourceSplit>> splitReaderSupplier,
      Configuration config,
      SourceReaderContext context) {
    super(splitReaderSupplier, new TiDBRecordEmitter(), config, context);
  }

  @Override
  protected void onSplitFinished(Map<String, TiDBSourceSplitState> map) {
  }

  @Override
  protected TiDBSourceSplitState initializedState(TiDBSourceSplit split) {
    return new TiDBSourceSplitState(split);
  }

  @Override
  protected TiDBSourceSplit toSplitType(String s, TiDBSourceSplitState state) {
    return state.toSplit();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    super.notifyCheckpointAborted(checkpointId);
  }
}
