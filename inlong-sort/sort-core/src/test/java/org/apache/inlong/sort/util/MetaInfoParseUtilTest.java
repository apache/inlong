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
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.MetaFieldInfo;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MongoExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.extract.OracleExtractNode;
import org.apache.inlong.sort.protocol.node.extract.PostgresExtractNode;
import org.apache.inlong.sort.protocol.node.extract.SqlServerExtractNode;
import org.apache.inlong.sort.protocol.node.format.CanalJsonFormat;
import org.apache.inlong.sort.protocol.node.format.JsonFormat;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test for {@link MetaInfoParseUtil}
 */
public class MetaInfoParseUtilTest {

    /**
     * Test Mongo meta field
     */
    @Test
    public void testMongoExtractNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("collection_name", MetaField.COLLECTION_NAME);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        MongoExtractNode mongoExtractNode = new MongoExtractNode("1", "mongo", fields,
                null, null, "test", "localhost:27017",
                "root", "inlong", "test");
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(mongoExtractNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'collection_name' VIRTUAL", sb.toString());
    }

    /**
     * Test Postgres meta field
     */
    @Test
    public void testPostgresExtractNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("database", MetaField.DATABASE_NAME);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        PostgresExtractNode postgresExtractNode = new PostgresExtractNode("1", "postgres_input", fields, null, null,
                null, Arrays.asList("user"), "localhost", "postgres", "inlong", "postgres", "public", 5432, null);
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(postgresExtractNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'database_name' VIRTUAL", sb.toString());
    }

    /**
     * Test Oracle meta field
     */
    @Test
    public void testOracleExtractNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("schema_name", MetaField.SCHEMA_NAME);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        OracleExtractNode oracleExtractNode = new OracleExtractNode("1", "oracle_input", fields,
                null, null, "ID", "localhost",
                "flinkuser", "flinkpw", "xE",
                "flinkuser", "table", 1521, null);
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(oracleExtractNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'schema_name' VIRTUAL", sb.toString());
    }

    /**
     * Test SQLServer meta field
     */
    @Test
    public void testSQLServerExtractNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("database", MetaField.DATABASE_NAME);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        SqlServerExtractNode sqlServerExtractNode = new SqlServerExtractNode("1", "sqlserver_out", fields, null, null,
                null, "localhost", 1433, "SA", "INLONG*123",
                "column_type_test", "dbo", "full_types", null);
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(sqlServerExtractNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'database_name' VIRTUAL", sb.toString());
    }

    /**
     * Test Kafka extract node meta field
     */
    @Test
    public void testKafkaExtractNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("value.database", MetaField.DATABASE_NAME);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        KafkaExtractNode kafkaExtractNode = new KafkaExtractNode("1", "kafka_input", fields, null, null, "workerJson",
                "localhost:9092", new CanalJsonFormat(), KafkaScanStartupMode.EARLIEST_OFFSET, null, "groupId");
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(kafkaExtractNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'value.database'", sb.toString());
    }

    /**
     * Test Kafka load node meta field
     */
    @Test
    public void testKafkaLoadNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("database", MetaField.DATABASE_NAME);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        List<FieldRelation> relations = Arrays
                .asList(new FieldRelation(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())),
                        new FieldRelation(new FieldInfo("age", new IntFormatInfo()),
                                new FieldInfo("age", new IntFormatInfo())));
        KafkaLoadNode kafkaLoadNode = new KafkaLoadNode("", "kafka_output", fields, relations, null, null,
                "workerJson", "localhost:9092",
                new JsonFormat(), null,
                null, null);
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(kafkaLoadNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'value.database'", sb.toString());
    }

    /**
     * Test MySQL meta field
     */
    @Test
    public void testMysqlExtractNodeMetaField() {
        MetaFieldInfo metaFieldInfo = new MetaFieldInfo("meta.op_type", MetaField.OP_TYPE);
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                metaFieldInfo
        );
        MySqlExtractNode mySqlExtractNode = new MySqlExtractNode("1", "mysql_input", fields, null, null,
                "id", Collections.singletonList("mysql_table"),
                "localhost", "inlong", "inlong",
                "inlong", null, null, null, null);
        StringBuilder sb = new StringBuilder();
        MetaInfoParseUtil.parseMetaField(mySqlExtractNode, metaFieldInfo, sb);
        Assert.assertEquals("STRING METADATA FROM 'meta.op_type' VIRTUAL", sb.toString());
    }

}
