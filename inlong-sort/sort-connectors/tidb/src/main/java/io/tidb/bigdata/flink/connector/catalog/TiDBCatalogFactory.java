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

package io.tidb.bigdata.flink.connector.catalog;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

/**
 * Factory for {@link TiDBCatalog}
 */
public class TiDBCatalogFactory implements CatalogFactory {
  
  public static final String IDENTIFIER = "tidb";
  
  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }
  
  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.of(
        TiDBOptions.DATABASE_URL,
        TiDBOptions.USERNAME
    );
  }
  
  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    // The options may less than real properties which tidb supported,
    // just use it by create catalog sql, we will not verify properties by flink api.
    return ImmutableSet.of(
        TiDBOptions.PASSWORD,
        TiDBOptions.MAX_POOL_SIZE,
        TiDBOptions.MIN_IDLE_SIZE,
        TiDBOptions.WRITE_MODE,
        TiDBOptions.REPLICA_READ,
        TiDBOptions.FILTER_PUSH_DOWN,
        TiDBOptions.DNS_SEARCH,
        TiDBOptions.SNAPSHOT_TIMESTAMP,
        TiDBOptions.SNAPSHOT_VERSION
    );
  }
  
  @Override
  public Catalog createCatalog(Context context) {
    return new TiDBCatalog(context.getName(), context.getOptions());
  }
  
}
