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

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test cases for {@link SQLServerSource}.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestSQLServerSource {

    @Mock
    JobProfile jobProfile;

    /**
     * Test cases for {@link SQLServerSource#split(JobProfile)}.
     */
    @Test
    public void testSplit() {
        
        final String sql1 = "select * from dbo.test01";
        final String sql2 = "select * from dbo.test${01,99}";

        // build mock
        when(jobProfile.get(eq(CommonConstants.PROXY_INLONG_GROUP_ID), anyString())).thenReturn("test_group");
        when(jobProfile.get(eq(CommonConstants.PROXY_INLONG_STREAM_ID), anyString())).thenReturn("test_stream");
        when(jobProfile.get(eq(SQLServerSource.JOB_DATABASE_SQL), eq(StringUtils.EMPTY))).thenReturn(StringUtils.EMPTY,
                sql1, sql2);

        final SQLServerSource source = new SQLServerSource();

        //assert
        assertEquals(null, source.split(jobProfile));
        assertEquals(1, source.split(jobProfile).size());
        assertEquals(99, source.split(jobProfile).size());
    }
}
