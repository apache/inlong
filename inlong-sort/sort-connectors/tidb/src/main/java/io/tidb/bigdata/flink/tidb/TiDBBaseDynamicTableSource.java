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

import io.tidb.bigdata.tidb.ClientConfig;
import java.util.Map;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TiDBBaseDynamicTableSource implements ScanTableSource, LookupTableSource {

  static final Logger LOG = LoggerFactory.getLogger(TiDBBaseDynamicTableSource.class);

  protected final TableSchema tableSchema;

  protected final Map<String, String> properties;

  protected final ClientConfig config;

  protected final JdbcLookupOptions lookupOptions;

  public TiDBBaseDynamicTableSource(TableSchema tableSchema, Map<String, String> properties,
      JdbcLookupOptions lookupOptions) {
    this.tableSchema = tableSchema;
    this.properties = properties;
    this.config = new ClientConfig(properties);
    this.lookupOptions = lookupOptions;
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    LOG.info("Use jdbc lookup table source");
    // JDBC only support non-nested look up keys
    String[] keyNames = new String[context.getKeys().length];
    for (int i = 0; i < keyNames.length; i++) {
      int[] innerKeyArr = context.getKeys()[i];
      Preconditions.checkArgument(innerKeyArr.length == 1,
          "JDBC only support non-nested look up keys");
      keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
    }
    RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
    return TableFunctionProvider.of(new JdbcRowDataLookupFunction(
        JdbcUtils.getJdbcOptions(properties),
        lookupOptions,
        tableSchema.getFieldNames(),
        tableSchema.getFieldDataTypes(),
        keyNames,
        rowType));
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public String asSummaryString() {
    return this.getClass().getName();
  }

  protected String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(properties.get(key), key + " can not be null");
  }

}
