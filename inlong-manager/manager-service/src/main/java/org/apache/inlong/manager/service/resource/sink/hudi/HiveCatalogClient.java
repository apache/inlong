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

package org.apache.inlong.manager.service.resource.sink.hudi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.org.apache.hadoop.hbase.types.DataType;
import org.apache.hudi.org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hudi.sync.common.util.ConfigUtils;
import org.apache.hudi.table.catalog.HiveSchemaUtils;
import org.apache.inlong.manager.pojo.sink.hudi.HudiColumnInfo;
import org.apache.inlong.manager.pojo.sink.hudi.HudiTableInfo;
import org.apache.thrift.TException;

public class HiveCatalogClient {

    private final String uri;
    private final String dbName;
    private final HiveMetaStoreClient client;

    public HiveCatalogClient(String uri, String dbName) throws MetaException {
        this.uri = uri;
        this.dbName = dbName;
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, uri);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
        this.client = new HiveMetaStoreClient(hiveConf);
    }

    public boolean dbExists() throws TException {
        List<String> allDatabases = client.getAllDatabases();
        if (allDatabases.isEmpty()) {
            return false;
        }
        return allDatabases.stream().anyMatch(db -> Objects.equals(db, dbName));
    }

    public void createDatabase(String warehouse, Map<String, String> meta) throws TException {
        Database database = new Database();
        Map<String, String> parameter = Maps.newHashMap();
        database.setLocationUri((new Path(warehouse, dbName) + ".db"));
        meta.forEach((key, value) -> {
            if (key.equals("comment")) {
                database.setDescription(value);
            } else if (key.equals("location")) {
                database.setLocationUri(value);
            } else if (value != null) {
                parameter.put(key, value);
            }
            database.setParameters(parameter);
        });
        client.createDatabase(database);
    }

    public void createDatabase(String warehouse) throws TException {
        createDatabase(warehouse, Maps.newHashMap());
    }

    public boolean tableExist(String tableName) throws TException {
        return client.tableExists(dbName, tableName);
    }

    public List<HudiColumnInfo> getColumns(String dbName, String tableName) throws TException {
        Table hiveTable = client.getTable(dbName, tableName);
        Schema schema = HiveSchemaUtils.convertTableSchema(hiveTable);
        List<HudiColumnInfo> columnList = new ArrayList<>();

        for (UnresolvedColumn column : schema.getColumns()) {
            UnresolvedPhysicalColumn physicalColumn = (UnresolvedPhysicalColumn) column;
            HudiColumnInfo info = new HudiColumnInfo();
            info.setName(physicalColumn.getName());
            DataType dataType = (DataType) physicalColumn.getDataType();
            info.setRequired(!dataType.isNullable());
            columnList.add(info);
        }
        return columnList;
    }

    public void addColumns(String tableName, List<HudiColumnInfo> columns) throws TException {
        Table hiveTable = client.getTable(dbName, tableName);
        Table newHiveTable = hiveTable.deepCopy();
        List<FieldSchema> cols = newHiveTable.getSd().getCols();
        for (HudiColumnInfo column : columns) {
            FieldSchema fieldSchema = new FieldSchema();
            fieldSchema.setName(column.getName());
            fieldSchema.setType(column.getType());
            fieldSchema.setComment(column.getDesc());
            cols.add(fieldSchema);
        }
        newHiveTable.getSd().setCols(cols);
        client.alter_table(dbName, tableName, newHiveTable);
    }

    public void createTable(String tableName, HudiTableInfo tableInfo, boolean useRealTimeInputFormat,
            Map<String, String> parameters)
            throws TException, IOException {
        Table hiveTable = org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(dbName, tableName);
        hiveTable.setOwner(UserGroupInformation.getCurrentUser().getUserName());
        hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

        Map<String, String> properties = new HashMap<>();
        String location = dbName + ".db" + "/" + tableName;
        properties.put("path", location);

        List<FieldSchema> cols = new ArrayList<>();
        for (HudiColumnInfo column : tableInfo.getColumns()) {
            FieldSchema fieldSchema = new FieldSchema();
            fieldSchema.setName(column.getName());
            fieldSchema.setType(column.getType());
            fieldSchema.setComment(column.getDesc());
            cols.add(fieldSchema);
        }

        // Build storage of hudi table
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(cols);
        hiveTable.setDbName(dbName);
        hiveTable.setTableName(tableName);
        // FIXME: splitSchemas need config by frontend

        HoodieFileFormat baseFileFormat = HoodieFileFormat.PARQUET;
        // ignore uber input Format
        String inputFormatClassName =
                HoodieInputFormatUtils.getInputFormatClassName(baseFileFormat, useRealTimeInputFormat);
        String outputFormatClassName = HoodieInputFormatUtils.getOutputFormatClassName(baseFileFormat);
        String serDeClassName = HoodieInputFormatUtils.getSerDeClassName(baseFileFormat);
        sd.setInputFormat(inputFormatClassName);
        sd.setOutputFormat(outputFormatClassName);
        Map<String, String> serdeProperties = new HashMap<>();
        serdeProperties.put("path", location);
        serdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(!useRealTimeInputFormat));
        serdeProperties.put("serialization.format", "1");

        sd.setSerdeInfo(new SerDeInfo(null, serDeClassName, serdeProperties));

        sd.setLocation(location);
        hiveTable.setSd(sd);

        hiveTable.setParameters(properties);

        client.createTable(hiveTable);
    }

}
