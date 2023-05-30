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

package org.apache.inlong.sort.kafka.partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The PrimaryKey Partitioner is used to extract primary key from single table messages:
 *
 * @param <T>
 */
public class SingleTablePrimaryKeyPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleTablePrimaryKeyPartitioner.class);
    private static final long serialVersionUID = 1L;

    /**
     * schema entry names which will be used to extract fields from schema for hashing.
     */
    private String partitionKey;

    /**
     * If a partitionNumber is specified, then use this number directly instead.
     */
    private int partitionNumber = -1;

    /**
     * the fieldnames of the schema
     */
    private String[] fieldNames;

    /**
     * the value field getters used to extract rowdata
     */
    private FieldGetter[] valueFieldGetters;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        super.open(parallelInstanceId, parallelInstances);
    }

    /**
     * precondition: record is of rowData Type, schema and partitionKey are not null
     */
    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");
        // parse the partition key fields
        return getPartition((RowData) record, partitions);
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    /**
     * input: rowdata,  partitions
     * output: the hashed partition number
     */
    private int getPartition(RowData data, int[] partitions) {
        if (partitionNumber >= 0) {
            return partitionNumber;
        }
        List<Integer> pos = getFieldPos();
        // parse out the List<String> from partitionkey and then get list of positions in schema.
        long hashCode = 0;
        for (int i : pos) {
            Object fieldValue = valueFieldGetters[i].getFieldOrNull(data);
            if (fieldValue != null) {
                hashCode += fieldValue.hashCode();
            }
        }
        return partitions[((int) ((hashCode & Integer.MAX_VALUE) % partitions.length))];
    }

    /**
     * output: integer list of partitionKeys' corresponding positions within schema
     */
    private List<Integer> getFieldPos() {
        // the positions of the partition keys.
        List<Integer> positions = new ArrayList<>();
        if (partitionKey == null) {
            LOG.error("primaryKeyPartioner:failed to fetch partitionKey");
            return positions;
        }
        // a map storing field names and their position in the schema
        HashMap<String, Integer> map = new HashMap<>();
        String[] keys = partitionKey.split(",");
        for (int i = 0; i < fieldNames.length; i++) {
            map.put(fieldNames[i], i);
        }
        for (String key : keys) {
            positions.add(map.get(key));
        }
        return positions;
    }

    @SuppressWarnings("deprecation")
    public void setSchema(TableSchema schema) {
        this.fieldNames = schema.getFieldNames();
    }

    public void setPartitionNumber(String partitionNumber) {
        // partition Number is optional parameter and not always needed, so it can be null.
        if (partitionNumber != null) {
            this.partitionNumber = Integer.parseInt(partitionNumber);
        }
    }

    public void setValueFieldGetters(FieldGetter[] fieldGetters) {
        this.valueFieldGetters = fieldGetters;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SingleTablePrimaryKeyPartitioner;
    }

    @Override
    public int hashCode() {
        return SingleTablePrimaryKeyPartitioner.class.hashCode();
    }
}
