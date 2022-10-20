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

import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.sources.reader.SQLServerReader;
import org.junit.Ignore;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertNotNull;

/**
 * Test cases for {@link SQLServerReader}.
 */
public class TestSQLServerConnect {

    /**
     * Just using in local test.
     */
    @Ignore
    public void testSQLServerReader() {
        JobProfile jobProfile = JobProfile.parseJsonStr("{}");
        jobProfile.set(SQLServerReader.JOB_DATABASE_USER, "sa");
        jobProfile.set(SQLServerReader.JOB_DATABASE_PASSWORD, "123456");
        jobProfile.set(SQLServerReader.JOB_DATABASE_HOSTNAME, "127.0.0.1");
        jobProfile.set(SQLServerReader.JOB_DATABASE_PORT, "1434");
        jobProfile.set(SQLServerReader.JOB_DATABASE_DBNAME, "inlong");
        final String sql = "select * from dbo.test01";
        jobProfile.set(SQLServerSource.JOB_DATABASE_SQL, sql);
        final SQLServerSource source = new SQLServerSource();
        List<Reader> readers = source.split(jobProfile);
        for (Reader reader : readers) {
            reader.init(jobProfile);
            while (!reader.isFinished()) {
                Message message = reader.read();
                if (Objects.nonNull(message)) {
                    assertNotNull(message.getBody());
                }
            }
        }
    }
}
