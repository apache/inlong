/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.hive.partition;

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HivePartitionInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo.HiveTimePartitionInfo;

/**
 * Copied from Apache Flink project.
 */
public class RowPartitionComputer implements PartitionComputer<Row> {

    private static final long serialVersionUID = 1L;

    protected final String defaultPartValue;
    protected final HivePartitionInfo[] partitionColumns;
    private final PartitionValueTransformer[] partitionValueTransformers;
    protected final int[] partitionIndexes;
    private final int[] nonPartitionIndexes;

    public RowPartitionComputer(
            String defaultPartValue, HiveSinkInfo hiveSinkInfo) {
        this.defaultPartValue = defaultPartValue;
        this.partitionColumns = hiveSinkInfo.getPartitions();
        this.partitionValueTransformers = new PartitionValueTransformer[partitionColumns.length];
        for (int i = 0; i < partitionValueTransformers.length; i++) {
            partitionValueTransformers[i] = generatePartitionValueTransformer(partitionColumns[i]);
        }
        final String[] partitionColumnNames = Arrays.stream(hiveSinkInfo.getPartitions())
                .map(HivePartitionInfo::getFieldName).toArray(String[]::new);
        List<String> columnList = Arrays.stream(hiveSinkInfo.getFields()).map(FieldInfo::getName)
                .collect(Collectors.toList());
        this.partitionIndexes =
                Arrays.stream(partitionColumnNames).mapToInt(columnList::indexOf).toArray();
        List<Integer> partitionIndexList =
                Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        this.nonPartitionIndexes =
                IntStream.range(0, columnList.size())
                        .filter(c -> !partitionIndexList.contains(c))
                        .toArray();
    }

    @Override
    public HivePartition generatePartValues(Row in) {
        final List<Tuple2<String, String>> partitions = new ArrayList<>();
        for (int i = 0; i < partitionIndexes.length; i++) {
            int index = partitionIndexes[i];
            Object field = in.getField(index);
            final String partitionValue;
            if (field == null) {
                partitionValue = defaultPartValue;
            } else {
                partitionValue = partitionValueTransformers[i].transform(field);
            }
            partitions.add(Tuple2.of(partitionColumns[i].getFieldName(), partitionValue));
        }
        final HivePartition hivePartition = new HivePartition();
        //noinspection unchecked
        hivePartition.setPartitions(partitions.toArray(new Tuple2[0]));
        return hivePartition;
    }

    @Override
    public Row projectColumnsToWrite(Row in) {
        return partitionIndexes.length == 0 ? in : Row.project(in, nonPartitionIndexes);
    }

    private PartitionValueTransformer generatePartitionValueTransformer(HivePartitionInfo hivePartitionInfo) {
        if (hivePartitionInfo instanceof HiveTimePartitionInfo) {
            return new TimeFieldPartitionValueTransformer((HiveTimePartitionInfo) hivePartitionInfo);
        } else {
            return new FieldPartitionValueTransformer();
        }
    }

    interface PartitionValueTransformer extends Serializable {

        String transform(Object fieldValue);
    }

    private static class FieldPartitionValueTransformer implements PartitionValueTransformer {

        private static final long serialVersionUID = 4704634852937539161L;

        @Override
        public String transform(Object fieldValue) {
            return String.valueOf(fieldValue);
        }
    }

    private static class TimeFieldPartitionValueTransformer implements PartitionValueTransformer {

        private static final long serialVersionUID = -6757777703443121967L;

        private final SimpleDateFormat dateFormat;

        public TimeFieldPartitionValueTransformer(HiveTimePartitionInfo hivePartitionInfo) {
            dateFormat = new SimpleDateFormat(hivePartitionInfo.getFormat());
        }

        @Override
        public String transform(Object fieldValue) {
            checkState(fieldValue instanceof Timestamp);
            return dateFormat.format((Date) fieldValue);
        }
    }
}
