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

package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.tidb.ClientConfig.DATABASE_URL;
import static io.tidb.bigdata.tidb.ClientConfig.MAX_POOL_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.MIN_IDLE_SIZE;
import static io.tidb.bigdata.tidb.ClientConfig.PASSWORD;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_DNS_SEARCH;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_FILTER_PUSH_DOWN;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static io.tidb.bigdata.tidb.ClientConfig.USERNAME;
//import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
//import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.factories.CatalogFactory;

public abstract class TiDBBaseCatalogFactory implements CatalogFactory {

  public static final String CATALOG_TYPE_VALUE_TIDB = "tidb";
  public static final String CATALOG_TYPE = "type";
  public static final String CATALOG_PROPERTY_VERSION = "property-version";
  public static final String CATALOG_DEFAULT_DATABASE = "default-database";

  @Override
  public Map<String, String> requiredContext() {
    return ImmutableMap.of(
        CATALOG_TYPE, CATALOG_TYPE_VALUE_TIDB,
        CATALOG_PROPERTY_VERSION, "1"
    );
  }

  @Override
  public List<String> supportedProperties() {
    return ImmutableList.of(
        USERNAME,
        PASSWORD,
        DATABASE_URL,
        MAX_POOL_SIZE,
        MIN_IDLE_SIZE,
        TIDB_WRITE_MODE,
        TIDB_REPLICA_READ,
        TIDB_FILTER_PUSH_DOWN,
        TIDB_DNS_SEARCH
    );
  }
}
