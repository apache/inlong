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

package org.apache.inlong.manager.pojo.sort.node;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.pojo.sort.node.extract.HudiFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.KafkaFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.MongoFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.MysqlBinlogFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.OracleFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.PostgreSqlFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.PulsarFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.RedisFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.SqlServerFactory;
import org.apache.inlong.manager.pojo.sort.node.extract.TubeMqFactory;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.node.ExtractNode;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The ExtractNode producer.
 */
public class NodeProducer {

    /**
     * The ExtractNode Factory Map
     */
    private static final Map<String, ExtractNodeFactory> EXTRACT_NODE_FACTORY_MAP = new HashMap<>(16);

    static {
        // The Factories Parsing SourceInfo to ExtractNode which sort needed
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.SQLSERVER, new SqlServerFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.POSTGRESQL, new PostgreSqlFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.MYSQL_BINLOG, new MysqlBinlogFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.HUDI, new HudiFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.KAFKA, new KafkaFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.MONGODB, new MongoFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.ORACLE, new OracleFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.PULSAR, new PulsarFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.REDIS, new RedisFactory());
        EXTRACT_NODE_FACTORY_MAP.put(SourceType.TUBEMQ, new TubeMqFactory());
    }

    /**
     * Create extract node by stream node info
     *
     * @param nodeInfo stream node info
     * @return extract node
     */
    public static ExtractNode createExtractNode(StreamNode nodeInfo) {
        StreamSource sourceInfo = (StreamSource) nodeInfo;
        String sourceType = sourceInfo.getSourceType();
        if (!EXTRACT_NODE_FACTORY_MAP.containsKey(sourceType)) {
            throw new IllegalArgumentException(
                    String.format("Unsupported sourceType=%s to create extractNode", sourceType));
        }
        return EXTRACT_NODE_FACTORY_MAP.get(sourceType).createNode(nodeInfo);
    }

    /**
     * Create extract nodes from the given sources.
     */
    public static List<ExtractNode> createExtractNodes(List<StreamSource> sourceInfos) {
        if (CollectionUtils.isEmpty(sourceInfos)) {
            return Lists.newArrayList();
        }
        return sourceInfos.stream().map(NodeProducer::createExtractNode)
                .collect(Collectors.toList());
    }
}
