/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.util;

import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.MetaFieldInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MongoExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.extract.OracleExtractNode;
import org.apache.inlong.sort.protocol.node.extract.PostgresExtractNode;
import org.apache.inlong.sort.protocol.node.extract.SqlServerExtractNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;

/**
 * Tool class of parsing meta field for different database
 */
public class MetaInfoParseUtil {

    /**
     * parse meta field for different database
     *
     * @param node data node info
     * @param metaFieldInfo meta field info
     * @param sb sql string append
     */
    public static void parseMetaField(Node node, MetaFieldInfo metaFieldInfo, StringBuilder sb) {
        if (metaFieldInfo.getMetaField() == MetaField.PROCESS_TIME) {
            sb.append(" AS PROCTIME()");
            return;
        }
        if (node instanceof MySqlExtractNode) {
            sb.append(parseMySqlExtractNodeMetaField(metaFieldInfo));
        } else if (node instanceof OracleExtractNode) {
            sb.append(parseOracleExtractNodeMetaField(metaFieldInfo));
        } else if (node instanceof KafkaExtractNode) {
            sb.append(parseKafkaExtractNodeMetaField(metaFieldInfo));
        } else if (node instanceof KafkaLoadNode) {
            sb.append(parseKafkaLoadNodeMetaField(metaFieldInfo));
        } else if (node instanceof PostgresExtractNode) {
            sb.append(parsePostgresExtractNodeMetaField(metaFieldInfo));
        } else if (node instanceof SqlServerExtractNode) {
            sb.append(parseSQLServerExtractNodeMetaField(metaFieldInfo));
        } else if (node instanceof MongoExtractNode) {
            sb.append(parseMongoExtractNodeMetaField(metaFieldInfo));
        } else {
            throw new UnsupportedOperationException(
                    String.format("This node:%s does not currently support metadata fields",
                            node.getClass().getName()));
        }
    }

    private static String parseKafkaLoadNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case TABLE_NAME:
                metaType = "STRING METADATA FROM 'value.table'";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'value.database'";
                break;
            case OP_TS:
                metaType = "TIMESTAMP(3) METADATA FROM 'value.event-timestamp'";
                break;
            case OP_TYPE:
                metaType = "STRING METADATA FROM 'value.op-type'";
                break;
            case DATA:
                metaType = "STRING METADATA FROM 'value.data'";
                break;
            case IS_DDL:
                metaType = "BOOLEAN METADATA FROM 'value.is-ddl'";
                break;
            case TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'value.ingestion-timestamp'";
                break;
            case SQL_TYPE:
                metaType = "MAP<STRING, INT> METADATA FROM 'value.sql-type'";
                break;
            case MYSQL_TYPE:
                metaType = "MAP<STRING, STRING> METADATA FROM 'value.mysql-type'";
                break;
            case PK_NAMES:
                metaType = "ARRAY<STRING> METADATA FROM 'value.pk-names'";
                break;
            case BATCH_ID:
                metaType = "BIGINT METADATA FROM 'value.batch-id'";
                break;
            case UPDATE_BEFORE:
                metaType = "ARRAY<MAP<STRING, STRING>> METADATA FROM 'value.update-before'";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for Kafka Load Node: %s",
                        metaFieldInfo.getMetaField()));
        }
        return metaType;
    }

    private static String parseKafkaExtractNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case TABLE_NAME:
                metaType = "STRING METADATA FROM 'value.table'";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'value.database'";
                break;
            case SQL_TYPE:
                metaType = "MAP<STRING, INT> METADATA FROM 'value.sql-type'";
                break;
            case PK_NAMES:
                metaType = "ARRAY<STRING> METADATA FROM 'value.pk-names'";
                break;
            case TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'value.ingestion-timestamp'";
                break;
            case OP_TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'value.event-timestamp'";
                break;
            // additional metadata
            case OP_TYPE:
                metaType = "STRING METADATA FROM 'value.op-type'";
                break;
            case IS_DDL:
                metaType = "BOOLEAN METADATA FROM 'value.is-ddl'";
                break;
            case MYSQL_TYPE:
                metaType = "MAP<STRING, STRING> METADATA FROM 'value.mysql-type'";
                break;
            case BATCH_ID:
                metaType = "BIGINT METADATA FROM 'value.batch-id'";
                break;
            case UPDATE_BEFORE:
                metaType = "ARRAY<MAP<STRING, STRING>> METADATA FROM 'value.update-before'";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for Kafka Extract Node: %s",
                        metaFieldInfo.getMetaField()));
        }
        return metaType;
    }

    private static String parseMySqlExtractNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case TABLE_NAME:
                metaType = "STRING METADATA FROM 'meta.table_name' VIRTUAL";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'meta.database_name' VIRTUAL";
                break;
            case OP_TS:
                metaType = "TIMESTAMP(3) METADATA FROM 'meta.op_ts' VIRTUAL";
                break;
            case OP_TYPE:
                metaType = "STRING METADATA FROM 'meta.op_type' VIRTUAL";
                break;
            case DATA:
                metaType = "STRING METADATA FROM 'meta.data' VIRTUAL";
                break;
            case IS_DDL:
                metaType = "BOOLEAN METADATA FROM 'meta.is_ddl' VIRTUAL";
                break;
            case TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'meta.ts' VIRTUAL";
                break;
            case SQL_TYPE:
                metaType = "MAP<STRING, INT> METADATA FROM 'meta.sql_type' VIRTUAL";
                break;
            case MYSQL_TYPE:
                metaType = "MAP<STRING, STRING> METADATA FROM 'meta.mysql_type' VIRTUAL";
                break;
            case PK_NAMES:
                metaType = "ARRAY<STRING> METADATA FROM 'meta.pk_names' VIRTUAL";
                break;
            case BATCH_ID:
                metaType = "BIGINT METADATA FROM 'meta.batch_id' VIRTUAL";
                break;
            case UPDATE_BEFORE:
                metaType = "ARRAY<MAP<STRING, STRING>> METADATA FROM 'meta.update_before' VIRTUAL";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for MySQL Extract Node: %s",
                        metaFieldInfo.getMetaField()));
        }
        return metaType;
    }

    private static String parseOracleExtractNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case TABLE_NAME:
                metaType = "STRING METADATA FROM 'table_name' VIRTUAL";
                break;
            case SCHEMA_NAME:
                metaType = "STRING METADATA FROM 'schema_name' VIRTUAL";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'database_name' VIRTUAL";
                break;
            case OP_TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for Oracle Extract Node: "
                                + "%s",
                        metaFieldInfo.getMetaField()));
        }
        return metaType;
    }

    private static String parsePostgresExtractNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case TABLE_NAME:
                metaType = "STRING METADATA FROM 'table_name' VIRTUAL";
                break;
            case SCHEMA_NAME:
                metaType = "STRING METADATA FROM 'schema_name' VIRTUAL";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'database_name' VIRTUAL";
                break;
            case OP_TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupport meta field for Postgres ExtractNode : %s",
                                metaFieldInfo.getMetaField()));
        }
        return metaType;
    }

    private static String parseSQLServerExtractNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case TABLE_NAME:
                metaType = "STRING METADATA FROM 'table_name' VIRTUAL";
                break;
            case SCHEMA_NAME:
                metaType = "STRING METADATA FROM 'schema_name' VIRTUAL";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'database_name' VIRTUAL";
                break;
            case OP_TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL";
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupport meta field for SQLServer ExtractNode: %s",
                                metaFieldInfo.getMetaField()));
        }
        return metaType;
    }

    private static String parseMongoExtractNodeMetaField(MetaFieldInfo metaFieldInfo) {
        String metaType;
        switch (metaFieldInfo.getMetaField()) {
            case COLLECTION_NAME:
                metaType = "STRING METADATA FROM 'collection_name' VIRTUAL";
                break;
            case DATABASE_NAME:
                metaType = "STRING METADATA FROM 'database_name' VIRTUAL";
                break;
            case OP_TS:
                metaType = "TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for MongoDB ExtractNode :"
                                + " %s",
                        metaFieldInfo.getMetaField()));
        }
        return metaType;
    }
}
