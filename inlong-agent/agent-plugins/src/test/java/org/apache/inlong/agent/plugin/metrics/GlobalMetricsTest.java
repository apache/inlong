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

package org.apache.inlong.agent.plugin.metrics;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class GlobalMetricsTest {

    private String groupId1 = "groupId_test1";
    private String groupId2 = "groupId_test2";
    private String streamId = "streamId";
    private String sinkTag = "File_Sink";
    private String sourceTag = "File_Source";

    @Test
    public void testPluginMetric() {
        String tag1 = groupId1 + "_" + streamId;
        String tag2 = groupId2 + "_" + streamId;
        GlobalMetrics.incReadNum(tag1);
        assertEquals(GlobalMetrics.getReadNum(tag1), 1);
        assertEquals(GlobalMetrics.getSendSuccessNum(tag2), 0);
        GlobalMetrics.incSendSuccessNum(tag2, 10);
        assertEquals(GlobalMetrics.getSendSuccessNum(tag2), 10);
        GlobalMetrics.incSendSuccessNum(tag2);
        assertEquals(GlobalMetrics.getSendSuccessNum(tag2), 11);
    }

    @Test
    public void testSinkMetric() {
        String tag = sinkTag + "_" + groupId1 + "_" + streamId;
        GlobalMetrics.incSinkFailCount(tag);
        assertEquals(GlobalMetrics.getSinkFailCount(tag), 1);
    }

    @Test
    public void testSourceMetric() {
        String tag1 = sourceTag + "_" + groupId1 + "_" + streamId;
        GlobalMetrics.incSourceSuccessCount(tag1);
        GlobalMetrics.incSourceSuccessCount(tag1);
        assertEquals(GlobalMetrics.getSourceSuccessCount(tag1), 2);

        String tag2 = sourceTag + "_" + groupId2 + "_" + streamId;
        assertEquals(GlobalMetrics.getSourceSuccessCount(tag2), 0);
        GlobalMetrics.incSourceSuccessCount(tag2);
        assertEquals(GlobalMetrics.getSourceSuccessCount(tag2), 1);

    }

}
