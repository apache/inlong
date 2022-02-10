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

package org.apache.inlong.sort.flink.clickhouse.output;

import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.flink.clickhouse.partitioner.BalancePartitioner;
import org.apache.inlong.sort.flink.clickhouse.partitioner.ClickHousePartitioner;
import org.apache.inlong.sort.flink.clickhouse.partitioner.HashPartitioner;
import org.apache.inlong.sort.flink.clickhouse.partitioner.RandomPartitioner;
import org.apache.inlong.sort.protocol.sink.ClickHouseSinkInfo;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseOutputFormatFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseOutputFormatFactory.class);

    /**
     * Get ClickHouseOutputFormat.
     * @param fieldNames names of fields.
     * @param formatInfos formatInfos of fields.
     * @param clickHouseSinkInfo {@link ClickHouseSinkInfo}.
     * @return {@link ClickHouseShardOutputFormat} for distributed table and
     *         {@link ClickHouseBatchOutputFormat} for normal table.
     */
    public static AbstractClickHouseOutputFormat generateClickHouseOutputFormat(
            String[] fieldNames,
            FormatInfo[] formatInfos,
            ClickHouseSinkInfo clickHouseSinkInfo) {
        if (clickHouseSinkInfo.getKeyFieldNames().length > 0) {
            LOG.warn("If primary key is specified, connector will be in UPSERT mode. "
                    + "You will have significant performance loss.");
        }

        if (clickHouseSinkInfo.isDistributedTable()) {
            ClickHousePartitioner partitioner;
            int partitionKeyIndex;

            switch (clickHouseSinkInfo.getPartitionStrategy()) {
                case BALANCE:
                    partitioner = new BalancePartitioner();
                    break;
                case RANDOM:
                    partitioner = new RandomPartitioner();
                    break;
                case HASH:
                    partitionKeyIndex = Arrays.asList(fieldNames).indexOf(clickHouseSinkInfo.getPartitionKey());
                    if (partitionKeyIndex == -1) {
                        throw new IllegalArgumentException("Partition key `" + clickHouseSinkInfo.getPartitionKey()
                                + "` not found in table schema");
                    }
                    partitioner = new HashPartitioner(partitionKeyIndex);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported partition strategy `"
                            + clickHouseSinkInfo.getPartitionStrategy() + "`");
            }

            return new ClickHouseShardOutputFormat(
                    fieldNames,
                    formatInfos,
                    partitioner,
                    clickHouseSinkInfo);
        }

        return new ClickHouseBatchOutputFormat(
                fieldNames,
                formatInfos,
                clickHouseSinkInfo);
    }
}
