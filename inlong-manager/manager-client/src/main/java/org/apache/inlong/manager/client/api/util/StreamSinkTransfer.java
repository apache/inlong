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

package org.apache.inlong.manager.client.api.util;

import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;

/**
 * Transfer util for stream sink.
 */
public class StreamSinkTransfer {

    /**
     * Parse stream sink
     */
    public static SinkRequest parseSinkRequest(SinkResponse sinkResponse) {
        SinkType sinkType = SinkType.forType(sinkResponse.getSinkType());
        switch (sinkType) {
            case KAFKA:
                return CommonBeanUtils.copyProperties((KafkaSinkResponse) sinkResponse, KafkaSinkRequest::new);
            case HIVE:
                return CommonBeanUtils.copyProperties((HiveSinkResponse) sinkResponse, HiveSinkRequest::new);
            case CLICKHOUSE:
                return CommonBeanUtils.copyProperties((ClickHouseSinkResponse) sinkResponse,
                        ClickHouseSinkRequest::new);
            case HBASE:
                return CommonBeanUtils.copyProperties((HbaseSinkResponse) sinkResponse, HbaseSinkRequest::new);
            case POSTGRES:
                return CommonBeanUtils.copyProperties((PostgresSinkResponse) sinkResponse, PostgresSinkRequest::new);
            default:
                throw new IllegalArgumentException(String.format("Unsupported sink type : %s for Inlong", sinkType));
        }
    }

}
