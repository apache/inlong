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

package org.apache.inlong.agent.plugin.sources;

import com.google.gson.Gson;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.SnapshotModeConstants;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.reader.PostgreSQLReader;
import org.apache.inlong.agent.pojo.DebeziumFormat;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_GROUP_ID;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_INLONG_STREAM_ID;

/**
 * test postgres reader
 */
public class PostgreSQLReaderTest {
    private static Gson GSON = new Gson();

    @Test
    public void testDebeziumFormat() {
        String debeziumJson = "{\n"
                + "    \"before\": null,\n"
                + "    \"after\": {\n"
                + "      \"id\": 1004,\n"
                + "      \"first_name\": \"Anne\",\n"
                + "      \"last_name\": \"Kretchmar\",\n"
                + "      \"email\": \"annek@noanswer.org\"\n"
                + "    },\n"
                + "    \"source\": {\n"
                + "      \"version\": \"12\",\n"
                + "      \"name\": \"myserver\",\n"
                + "      \"ts_sec\": 0,\n"
                + "      \"gtid\": null,\n"
                + "      \"file\": \"000000010000000000000001\",\n"
                + "      \"row\": 0,\n"
                + "      \"snapshot\": true,\n"
                + "      \"thread\": null,\n"
                + "      \"db\": \"postgres\",\n"
                + "      \"table\": \"customers\"\n"
                + "    },\n"
                + "    \"op\": \"r\",\n"
                + "    \"ts_ms\": 1486500577691\n"
                + "  }";
        DebeziumFormat debeziumFormat = GSON
                .fromJson(debeziumJson, DebeziumFormat.class);
        Assert.assertEquals("customers", debeziumFormat.getSource().getTable());
        Assert.assertEquals("true", debeziumFormat.getSource().getSnapshot());
    }

//    @Test
    public void postgresLoadTest() {
        JobProfile jobProfile = new JobProfile();
        jobProfile.set(PostgreSQLReader.JOB_POSTGRESQL_USER, "postgres");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_SERVER_NAME, "postgres");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_PLUGIN_NAME, "pgoutput");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_PASSWORD, "123456");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_HOSTNAME, "localhost");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_PORT, "5432");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_OFFSET_SPECIFIC_OFFSET_FILE, "000000010000000000000001");
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_SNAPSHOT_MODE, SnapshotModeConstants.INITIAL);
        jobProfile.set(PostgreSQLReader.JOB_DATABASE_DBNAME, "postgres");
        jobProfile.set("job.instance.id", "_1");
        jobProfile.set(PROXY_INLONG_GROUP_ID, "groupid");
        jobProfile.set(PROXY_INLONG_STREAM_ID, "streamid");
        PostgreSQLReader postgreSqlReader = new PostgreSQLReader();
        postgreSqlReader.init(jobProfile);
        while (true) {
            Message message = postgreSqlReader.read();
            if (message != null) {
                System.out.println(message.toString());
            }
        }
    }
}
