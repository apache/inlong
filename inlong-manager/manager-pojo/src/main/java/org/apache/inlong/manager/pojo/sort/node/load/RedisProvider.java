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

package org.apache.inlong.manager.pojo.sort.node.load;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.pojo.sink.redis.RedisSink;
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.format.AvroFormat;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.CsvFormat;
import org.apache.inlong.sort.protocol.node.format.DebeziumJsonFormat;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.node.format.InLongMsgFormat;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.format.RawFormat;
import org.apache.inlong.sort.protocol.node.load.RedisLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * The Provider for creating Redis load nodes.
 */
public class RedisProvider implements LoadNodeProvider {

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.REDIS.equals(sinkType);
    }

    @Override
    public LoadNode createNode(StreamNode nodeInfo, Map<String, StreamField> constantFieldMap) {
        RedisSink redisSink = (RedisSink) nodeInfo;
        Map<String, String> properties = parseProperties(redisSink.getProperties());
        List<FieldInfo> fieldInfos = parseFieldInfos(redisSink.getSinkFieldList(), redisSink.getSinkName());
        List<FieldRelation> fieldRelations = parseSinkFields(redisSink.getSinkFieldList(), constantFieldMap);

        String clusterMode = redisSink.getClusterMode();
        String dataType = redisSink.getDataType();
        String schemaMapMode = redisSink.getSchemaMapMode();
        String host = redisSink.getHost();
        Integer port = redisSink.getPort();
        String clusterNodes = redisSink.getClusterNodes();
        String masterName = redisSink.getMasterName();
        String sentinelsInfo = redisSink.getSentinelsInfo();
        Integer database = redisSink.getDatabase();
        String password = redisSink.getPassword();
        Integer ttl = redisSink.getTtl();
        Integer timeout = redisSink.getTimeout();
        Integer soTimeout = redisSink.getSoTimeout();
        Integer maxTotal = redisSink.getMaxTotal();
        Integer maxIdle = redisSink.getMaxIdle();
        Integer minIdle = redisSink.getMinIdle();
        Integer maxRetries = redisSink.getMaxRetries();

        Format format = parsingFormat(
                redisSink.getFormatDataType(),
                false,
                redisSink.getFormatDataSeparator(),
                false);

        return new RedisLoadNode(
                redisSink.getSinkName(),
                redisSink.getSinkName(),
                fieldInfos,
                fieldRelations,
                null,
                null,
                null,
                properties,
                clusterMode,
                dataType,
                schemaMapMode,
                host,
                port,
                clusterNodes,
                masterName,
                sentinelsInfo,
                database,
                password,
                ttl,
                format,
                timeout,
                soTimeout,
                maxTotal,
                maxIdle,
                minIdle,
                maxRetries);
    }

    /**
     * Parse format
     *
     * @param formatName data serialization, support: csv, json, canal, avro, etc
     * @param wrapWithInlongMsg whether wrap content with {@link InLongMsgFormat}
     * @param separatorStr the separator of data content
     * @param ignoreParseErrors whether ignore deserialization error data
     * @return the format for serialized content
     */
    private static Format parsingFormat(
            String formatName,
            boolean wrapWithInlongMsg,
            String separatorStr,
            boolean ignoreParseErrors) {
        Format format;
        DataTypeEnum dataType = DataTypeEnum.forType(formatName);
        switch (dataType) {
            case CSV:
                if (StringUtils.isNumeric(separatorStr)) {
                    char dataSeparator = (char) Integer.parseInt(separatorStr);
                    separatorStr = Character.toString(dataSeparator);
                }
                CsvFormat csvFormat = new CsvFormat(separatorStr);
                csvFormat.setIgnoreParseErrors(ignoreParseErrors);
                format = csvFormat;
                break;
            case AVRO:
                format = new AvroFormat();
                break;
            case JSON:
                JsonFormat jsonFormat = new JsonFormat();
                jsonFormat.setIgnoreParseErrors(ignoreParseErrors);
                format = jsonFormat;
                break;
            case CANAL:
                format = new CanalJsonFormat();
                break;
            case DEBEZIUM_JSON:
                DebeziumJsonFormat debeziumJsonFormat = new DebeziumJsonFormat();
                debeziumJsonFormat.setIgnoreParseErrors(ignoreParseErrors);
                format = debeziumJsonFormat;
                break;
            case RAW:
                format = new RawFormat();
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported dataType=%s", dataType));
        }
        if (wrapWithInlongMsg) {
            Format innerFormat = format;
            format = new InLongMsgFormat(innerFormat, false);
        }
        return format;
    }
}