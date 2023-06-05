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

package org.apache.inlong.manager.pojo.sort.node.extract;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.pojo.sort.node.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.source.redis.RedisLookupOptions;
import org.apache.inlong.manager.pojo.source.redis.RedisSource;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.LookupOptions;
import org.apache.inlong.sort.protocol.enums.RedisCommand;
import org.apache.inlong.sort.protocol.enums.RedisMode;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.RedisExtractNode;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Redis extract nodes.
 */
public class RedisProvider implements ExtractNodeProvider {

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.REDIS.equals(sourceType);
    }

    @Override
    public ExtractNode createNode(StreamNode streamNodeInfo) {
        RedisSource source = (RedisSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseFieldInfos(source.getFieldList(), source.getSourceName());
        Map<String, String> properties = parseProperties(source.getProperties());

        RedisMode redisMode = RedisMode.forName(source.getRedisMode());
        switch (redisMode) {
            case STANDALONE:
                return new RedisExtractNode(
                        source.getSourceName(),
                        source.getSourceName(),
                        fieldInfos,
                        null,
                        properties,
                        source.getPrimaryKey(),
                        RedisCommand.forName(source.getCommand()),
                        source.getHost(),
                        source.getPort(),
                        source.getPassword(),
                        source.getAdditionalKey(),
                        source.getDatabase(),
                        source.getTimeout(),
                        source.getSoTimeout(),
                        source.getMaxTotal(),
                        source.getMaxIdle(),
                        source.getMinIdle(),
                        parseLookupOptions(source.getLookupOptions()));
            case SENTINEL:
                return new RedisExtractNode(
                        source.getSourceName(),
                        source.getSourceName(),
                        fieldInfos,
                        null,
                        properties,
                        source.getPrimaryKey(),
                        RedisCommand.forName(source.getCommand()),
                        source.getMasterName(),
                        source.getSentinelsInfo(),
                        source.getPassword(),
                        source.getAdditionalKey(),
                        source.getDatabase(),
                        source.getTimeout(),
                        source.getSoTimeout(),
                        source.getMaxTotal(),
                        source.getMaxIdle(),
                        source.getMinIdle(),
                        parseLookupOptions(source.getLookupOptions()));
            case CLUSTER:
                return new RedisExtractNode(
                        source.getSourceName(),
                        source.getSourceName(),
                        fieldInfos,
                        null,
                        properties,
                        source.getPrimaryKey(),
                        RedisCommand.forName(source.getCommand()),
                        source.getClusterNodes(),
                        source.getPassword(),
                        source.getAdditionalKey(),
                        source.getDatabase(),
                        source.getTimeout(),
                        source.getSoTimeout(),
                        source.getMaxTotal(),
                        source.getMaxIdle(),
                        source.getMinIdle(),
                        parseLookupOptions(source.getLookupOptions()));
            default:
                throw new IllegalArgumentException(String.format("Unsupported redis-mode=%s for Inlong", redisMode));
        }
    }

    /**
     * Parse LookupOptions
     *
     * @param options RedisLookupOptions
     * @return LookupOptions
     */
    private static LookupOptions parseLookupOptions(RedisLookupOptions options) {
        if (options == null) {
            return null;
        }
        return new LookupOptions(options.getLookupCacheMaxRows(), options.getLookupCacheTtl(),
                options.getLookupMaxRetries(), options.getLookupAsync());
    }
}