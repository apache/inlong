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

package org.apache.inlong.sort.singletenant.flink.parser.impl;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.protocol.BuiltInFieldInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.GroupInfo;
import org.apache.inlong.sort.protocol.StreamInfo;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.transform.TransformNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.apache.inlong.sort.singletenant.flink.parser.Parser;
import org.apache.inlong.sort.singletenant.flink.parser.result.FlinkSqlParseResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Flink sql parse handler
 * It accepts a Tableenv and GroupInfo, and outputs the parsed FlinkSqlParseResult
 */
public class FlinkSqlParser implements Parser {

    private static final Logger log = LoggerFactory.getLogger(FlinkSqlParser.class);

    private final TableEnvironment tableEnv;
    private final GroupInfo groupInfo;
    private final Set<String> hasParsedSet = new HashSet<>();
    private final Map<String, String> extractTableSqls = new TreeMap<>();
    private final Map<String, String> transformTableSqls = new TreeMap<>();
    private final Map<String, String> loadTableSqls = new TreeMap<>();
    private final List<String> insertSqls = new ArrayList<>();

    /**
     * Flink sql parse constructor
     *
     * @param tableEnv The tableEnv,it is the execution environment of flink sql
     * @param groupInfo The groupInfo,it is the data model abstraction of task execution
     */
    public FlinkSqlParser(TableEnvironment tableEnv, GroupInfo groupInfo) {
        this.tableEnv = tableEnv;
        this.groupInfo = groupInfo;
    }

    /**
     * Get a instance of FlinkSqlParser
     *
     * @param tableEnv The tableEnv,it is the execution environment of flink sql
     * @param groupInfo The groupInfo,it is the data model abstraction of task execution
     * @return FlinkSqlParser The flink sql parse handler
     */
    public static FlinkSqlParser getInstance(TableEnvironment tableEnv, GroupInfo groupInfo) {
        return new FlinkSqlParser(tableEnv, groupInfo);
    }

    /**
     * GroupInfo parse,it accepts a Tableenv and GroupInfo, and outputs the parsed FlinkSqlParseResult
     *
     * @return FlinkSqlParseResult the result of flink sql parse
     */
    @Override
    public FlinkSqlParseResult parse() {
        Preconditions.checkNotNull(groupInfo, "group info is null");
        Preconditions.checkNotNull(groupInfo.getStreams(), "streams is null");
        Preconditions.checkState(!groupInfo.getStreams().isEmpty(), "streams is empty");
        Preconditions.checkNotNull(tableEnv, "tableEnv is null");
        log.info("start parse group, groupId:{}", groupInfo.getGroupId());
        for (StreamInfo streamInfo : groupInfo.getStreams()) {
            parseStream(streamInfo);
        }
        log.info("parse group success, groupId:{}", groupInfo.getGroupId());
        List<String> createTableSqls = new ArrayList<>(extractTableSqls.values());
        createTableSqls.addAll(transformTableSqls.values());
        createTableSqls.addAll(loadTableSqls.values());
        return new FlinkSqlParseResult(tableEnv, createTableSqls, insertSqls);
    }

    private void parseStream(StreamInfo streamInfo) {
        Preconditions.checkNotNull(streamInfo, "stream is null");
        Preconditions.checkNotNull(streamInfo.getStreamId(), "streamId is null");
        Preconditions.checkNotNull(streamInfo.getNodes(), "nodes is null");
        Preconditions.checkState(!streamInfo.getNodes().isEmpty(), "nodes is empty");
        Preconditions.checkNotNull(streamInfo.getRelations(), "relations is null");
        Preconditions.checkState(!streamInfo.getRelations().isEmpty(), "relations is empty");
        log.info("start parse stream,streamId:{}", streamInfo.getStreamId());
        Map<String, Node> nodeMap = new HashMap<>(streamInfo.getNodes().size());
        streamInfo.getNodes().forEach(s -> {
            Preconditions.checkNotNull(s.getId(), "node id is null");
            nodeMap.put(s.getId(), s);
        });
        Map<String, NodeRelationShip> relationMap = new HashMap<String, NodeRelationShip>();
        streamInfo.getRelations().forEach(r -> {
            for (String output : r.getOutputs()) {
                relationMap.put(output, r);
            }
        });
        streamInfo.getRelations().forEach(r -> {
            parseNodeRelation(r, nodeMap, relationMap);
        });
        log.info("parse stream success, streamId:{}", streamInfo.getStreamId());
    }

    private void parseNodeRelation(NodeRelationShip relation, Map<String, Node> nodeMap,
            Map<String, NodeRelationShip> relationMap) {
        log.info("start parse node relation,relation:{}", relation);
        Preconditions.checkNotNull(relation, "relation is null");
        Preconditions.checkState(relation.getInputs().size() > 0,
                "relation must have at least one input node");
        Preconditions.checkState(relation.getOutputs().size() > 0,
                "relation must have at least one output node");
        relation.getOutputs().forEach(s -> {
            Preconditions.checkNotNull(s, "node id in outputs is null");
            Node node = nodeMap.get(s);
            Preconditions.checkNotNull(node, "can not find any node by node id " + s);
            parseNode(node, relation, nodeMap, relationMap);
        });
        log.info("parse node relation success, relation:{}", relation);
    }

    private void registerTableSql(Node node, String sql) {
        if (node instanceof ExtractNode) {
            extractTableSqls.put(node.getId(), sql);
        } else if (node instanceof TransformNode) {
            transformTableSqls.put(node.getId(), sql);
        } else if (node instanceof LoadNode) {
            loadTableSqls.put(node.getId(), sql);
        } else {
            throw new UnsupportedOperationException("Only support [ExtractNode|TransformNode|LoadNode]");
        }
    }

    private void parseNode(Node node, NodeRelationShip relation, Map<String, Node> nodeMap,
            Map<String, NodeRelationShip> relationMap) {
        if (hasParsedSet.contains(node.getId())) {
            log.warn("the node has already been parsed, node id:{}", node.getId());
            return;
        }
        if (node instanceof ExtractNode) {
            log.info("start parse node, node id:{}", node.getId());
            String sql = genCreateSql(node);
            log.info("node id:{}, create table sql:\n{}", node.getId(), sql);
            registerTableSql(node, sql);
            hasParsedSet.add(node.getId());
        } else {
            Preconditions.checkNotNull(relation, "relation is null");
            for (String upstreamNodeId : relation.getInputs()) {
                if (!hasParsedSet.contains(upstreamNodeId)) {
                    Node upstreamNode = nodeMap.get(upstreamNodeId);
                    Preconditions.checkNotNull(upstreamNode,
                            "can not find any node by node id " + upstreamNodeId);
                    parseNode(upstreamNode, relationMap.get(upstreamNodeId), nodeMap, relationMap);
                }
            }
            if (node instanceof LoadNode) {
                String createSql = genCreateSql(node);
                log.info("node id:{}, create table sql:\n{}", node.getId(), createSql);
                registerTableSql(node, createSql);
                Preconditions.checkState(relation.getInputs().size() == 1,
                        "load node only support one input node");
                LoadNode loadNode = (LoadNode) node;
                String insertSql = genLoadNodeInsertSql(loadNode, nodeMap.get(relation.getInputs().get(0)));
                log.info("insert sql => {}, node id:{}", insertSql, node.getId());
                insertSqls.add(insertSql);
                hasParsedSet.add(node.getId());
            } else if (node instanceof TransformNode) {
                TransformNode transformNode = (TransformNode) node;
                Preconditions.checkNotNull(transformNode.getFieldRelationShips(), "field relations is null");
                Preconditions.checkState(!transformNode.getFieldRelationShips().isEmpty(),
                        "field relations is empty");
                Preconditions.checkState(relation.getInputs().size() == 1,
                        "simple transform only support one input node");
                Preconditions.checkState(relation.getOutputs().size() == 1,
                        "join node only support one output node");
                String selectSql = genSimpleTransformSelectSql(transformNode, relation, nodeMap);
                String createSql = genCreateSql(node);
                log.info("node id:{}, create table sql:\n{}", node.getId(), createSql);
                Preconditions.checkState(relation.getInputs().size() == 1,
                        "load node only support one input node");
                log.info("tansform sql => {}, node id:{}", selectSql, node.getId());
                registerTableSql(node, createSql + " AS " + selectSql);
                hasParsedSet.add(node.getId());
            }
        }
        log.info("parse node success, node id:{}", node.getId());
    }

    private String genSimpleTransformSelectSql(TransformNode node,
            NodeRelationShip relation, Map<String, Node> nodeMap) {
        Preconditions.checkNotNull(node.getFieldRelationShips(), "field relations is null");
        Preconditions.checkState(!node.getFieldRelationShips().isEmpty(),
                "field relations is empty");
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        Map<String, FieldRelationShip> fieldRelationMap = new HashMap<>(node.getFieldRelationShips().size());
        node.getFieldRelationShips().forEach(s -> {
            fieldRelationMap.put(s.getOutputField().getName(), s);
        });
        for (FieldInfo field : node.getFields()) {
            FieldRelationShip fieldRelation = fieldRelationMap.get(field.getName());
            if (fieldRelation != null) {
                sb.append(fieldRelation.getInputField().format()).append(" AS ").append(field.format()).append(",");
            } else {
                String targetType = TableFormatUtils.deriveLogicalType(field.getFormatInfo()).asSerializableString();
                sb.append("CAST(NULL as ").append(targetType).append(") AS ").append(field.format()).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" FROM `").append(nodeMap.get(relation.getInputs().get(0)).genTableName()).append("` ");
        if (node.getFilters() != null && !node.getFilters().isEmpty()) {
            sb.append("\n WHERE");
            for (FilterFunction filter : node.getFilters()) {
                sb.append(" ").append(filter.format());
            }
        }
        return sb.toString();
    }

    private String genLoadNodeInsertSql(LoadNode loadNode, Node inputNode) {
        Preconditions.checkNotNull(loadNode.getFieldRelationShips(), "field relations is null");
        Preconditions.checkState(!loadNode.getFieldRelationShips().isEmpty(),
                "field relations is empty");
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO `").append(loadNode.genTableName()).append("` ");
        sb.append("SELECT ");
        Map<String, FieldRelationShip> fieldRelationMap = new HashMap<>(loadNode.getFieldRelationShips().size());
        loadNode.getFieldRelationShips().forEach(s -> {
            fieldRelationMap.put(s.getOutputField().getName(), s);
        });
        for (FieldInfo field : loadNode.getFields()) {
            FieldRelationShip fieldRelation = fieldRelationMap.get(field.getName());
            if (fieldRelation != null) {
                sb.append(fieldRelation.getInputField().format()).append(" AS ").append(field.format()).append(",");
            } else {
                String targetType = TableFormatUtils.deriveLogicalType(field.getFormatInfo()).asSerializableString();
                sb.append("CAST(NULL as ").append(targetType).append(") AS ").append(field.format()).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(" FROM `").append(inputNode.genTableName()).append("`");
        if (loadNode.getFilters() != null && !loadNode.getFilters().isEmpty()) {
            sb.append(" WHERE");
            for (FilterFunction filter : loadNode.getFilters()) {
                sb.append(" ").append(filter.format());
            }
        }
        return sb.toString();
    }

    private String genCreateSql(Node node) {
        if (node instanceof TransformNode) {
            return genCreateTransformSql(node);
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE `");
        sb.append(node.genTableName()).append("`(\n");
        sb.append(genPrimaryKey(node.getPrimaryKey()));
        sb.append(parseFields(node.getFields(), node instanceof LoadNode));
        if (node instanceof ExtractNode) {
            ExtractNode extractNode = (ExtractNode) node;
            if (extractNode.getWatermarkField() != null) {
                sb.append(",\n     ").append(extractNode.getWatermarkField().format());
            }
        }
        sb.append(")");
        if (node.getPartitionFields() != null && !node.getPartitionFields().isEmpty()) {
            sb.append(String.format("\nPARTITIONED BY (%s)",
                    StringUtils.joinWith(",", formatFields(node.getPartitionFields()))));
        }
        sb.append(parseOptions(node.tableOptions()));
        return sb.toString();
    }

    private String genCreateTransformSql(Node node) {
        return String.format("CREATE VIEW `%s` (\n%s)",
                node.genTableName(), parseTransformNodeFields(node.getFields()));
    }

    private String parseOptions(Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        if (options != null && !options.isEmpty()) {
            sb.append("\n    WITH (");
            for (Map.Entry<String, String> kv : options.entrySet()) {
                sb.append("\n    '").append(kv.getKey()).append("' = '").append(kv.getValue()).append("'").append(",");
            }
            if (sb.length() > 0) {
                sb.delete(sb.lastIndexOf(","), sb.length());
            }
            sb.append("\n)");
        }
        return sb.toString();
    }

    public String parseTransformNodeFields(List<FieldInfo> fields) {
        StringBuilder sb = new StringBuilder();
        for (FieldInfo field : fields) {
            sb.append("    `").append(field.getName()).append("`,");
        }
        if (sb.length() > 0) {
            sb.delete(sb.lastIndexOf(","), sb.length());
        }
        return sb.toString();
    }

    public String parseFields(List<FieldInfo> fields, boolean isLoad) {
        StringBuilder sb = new StringBuilder();
        for (FieldInfo field : fields) {
            sb.append("    `").append(field.getName()).append("` ");
            if (field instanceof BuiltInFieldInfo) {
                throw new UnsupportedOperationException("Metadata field sync is not currently supported");
            } else {
                sb.append(TableFormatUtils.deriveLogicalType(field.getFormatInfo()).asSerializableString());
            }
            sb.append(",\n");
        }
        if (sb.length() > 0) {
            sb.delete(sb.lastIndexOf(","), sb.length());
        }
        return sb.toString();
    }

    private String genPrimaryKey(String primaryKey) {
        if (StringUtils.isNotBlank(primaryKey)) {
            primaryKey = String.format("    PRIMARY KEY (%s) NOT ENFORCED,\n",
                    StringUtils.join(formatFields(primaryKey.split(",")), ","));
        } else {
            primaryKey = "";
        }
        return primaryKey;
    }

    private List<String> formatFields(String... fields) {
        List<String> formatFields = new ArrayList<>(fields.length);
        for (String field : fields) {
            if (!field.contains("`")) {
                formatFields.add(String.format("`%s`", field.trim()));
            } else {
                formatFields.add(field);
            }
        }
        return formatFields;
    }

    private List<String> formatFields(List<FieldInfo> fields) {
        List<String> formatFields = new ArrayList<>(fields.size());
        for (FieldInfo field : fields) {
            formatFields.add(field.format());
        }
        return formatFields;
    }
}
