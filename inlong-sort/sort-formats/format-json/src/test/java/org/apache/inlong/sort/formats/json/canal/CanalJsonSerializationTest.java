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

package org.apache.inlong.sort.formats.json.canal;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.ddl.Column;
import org.apache.inlong.sort.ddl.Position;
import org.apache.inlong.sort.ddl.enums.AlterType;
import org.apache.inlong.sort.ddl.enums.PositionType;
import org.apache.inlong.sort.ddl.expressions.AlterColumn;
import org.apache.inlong.sort.ddl.operations.AlterOperation;
import org.apache.inlong.sort.ddl.operations.DropTableOperation;
import org.apache.inlong.sort.ddl.operations.Operation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalJsonSerializationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CanalJsonSerializationTest.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testCanalJsonSerialization() {

        List<AlterColumn> alterColumns = new ArrayList<>();

        Column column = new Column("columnDataType.getColumnName()", new ArrayList<>(),
                1,
                new Position(PositionType.FIRST, null), true, "23",
                "23");

        alterColumns.add(new AlterColumn(AlterType.ADD_COLUMN, column, null));

        AlterOperation alterOperation = new AlterOperation(alterColumns);

        DropTableOperation dropTableOperation = new DropTableOperation();

        CanalJson canalJson = CanalJson.builder()
                .data(null)
                .es(0)
                .table("table")
                .type("type")
                .database("database")
                .ts(0)
                .sql("sql")
                .mysqlType(null)
                .sqlType(null)
                .pkNames(null)
                .schema("schema")
                .oracleType(null)
                .operation(alterOperation)
                .incremental(false)
                .build();

        ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        try {
            String writeValueAsString = OBJECT_MAPPER.writeValueAsString(canalJson);
            LOG.info(writeValueAsString);
            CanalJson canalJson1 = objectMapper.readValue(writeValueAsString, CanalJson.class);

            String ss = OBJECT_MAPPER.writeValueAsString(alterOperation);
            LOG.info(ss);
            Operation operation = objectMapper.readValue(ss, Operation.class);

            String drop = OBJECT_MAPPER.writeValueAsString(dropTableOperation);
            LOG.info(drop);
            Operation dropTable = objectMapper.readValue(drop, Operation.class);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
