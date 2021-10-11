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

package org.apache.inlong.sort.flink.tubemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.TestLogger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Unit test for {@link TubeSubscriptionDescription}.
 */
public class TubeSubscriptionDescriptionTest extends TestLogger {

    private final String topic = "topic";

    private final String masterAddress = "localhost:9527";

    private final String tid = "10001";

    private final long dataFlowId = 9527L;

    @Test
    public void testCopyConstructor() {
        final TubeSubscriptionDescription description = new TubeSubscriptionDescription(topic, masterAddress, null);
        final TubeSourceInfo tubeSourceInfo = new TubeSourceInfo(
                topic,
                masterAddress,
                null,
                new TDMsgCsvDeserializationInfo(tid, ',', true),
                new FieldInfo[0]);
        description.addDataFlow(dataFlowId, tubeSourceInfo);

        final TubeSubscriptionDescription copied = new TubeSubscriptionDescription(description);
        assertEquals(description, copied);

        copied.removeDataFlow(dataFlowId);
        assertNotEquals(description, copied);
    }

    @Test
    public void testAddAndRemoveDataFlow() {
        final TubeSubscriptionDescription description = new TubeSubscriptionDescription(topic, masterAddress, null);
        final TubeSourceInfo tubeSourceInfo1 = new TubeSourceInfo(
                topic,
                masterAddress,
                null,
                new TDMsgCsvDeserializationInfo(tid, ',', true),
                new FieldInfo[0]);
        description.addDataFlow(dataFlowId, tubeSourceInfo1);
        final String tid2 = "10002";
        final String consumerGroup = "consumerGroup";
        final TubeSourceInfo tubeSourceInfo2 = new TubeSourceInfo(
                topic,
                masterAddress,
                consumerGroup,
                new TDMsgCsvDeserializationInfo(tid2, ',', true),
                new FieldInfo[0]);
        final long dataFlowId2 = 10086L;
        description.addDataFlow(dataFlowId2, tubeSourceInfo2);
        final List<String> expectedTids = new ArrayList<>();
        expectedTids.add(tid);
        expectedTids.add(tid2);
        Collections.sort(expectedTids);
        final List<String> tids = description.getTids();
        Collections.sort(tids);
        assertEquals(expectedTids, tids);

        description.removeDataFlow(dataFlowId);
        assertEquals(Collections.singletonList(tid2), description.getTids());

        description.removeDataFlow(dataFlowId2);
        try {
            description.getTids();
            fail("Should throw exception when get tids from an empty description");
        } catch (IllegalStateException expected) {

        }
    }
}
