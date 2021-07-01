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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.flink.tubemq.MultiTopicTubeSourceFunction.SourceEvent;
import org.apache.inlong.sort.flink.tubemq.MultiTopicTubeSourceFunction.SourceEventType;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.apache.inlong.sort.util.TestLogger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Unit test for {@link MultiTopicTubeSourceFunction}.
 */
public class MultiTopicTubeSourceFunctionTest extends TestLogger {

    private final String topicAdded = "topicAdded";

    private final String topicRemoved = "topicRemoved";

    private final String masterAddress = "localhost:9527";

    private final String tid1 = "10001";

    private final String tid2 = "10002";

    private final long dataFlowId1 = 9527L;

    private final long dataFlowId2 = 9528L;

    private final long dataFlowId3 = 9529L;

    private TestingMultiTenancyTubeConsumer generateTubeConsumer() {
        final Configuration configuration = new Configuration();
        configuration.setString(Constants.CLUSTER_ID, "cluster_id");
        return new TestingMultiTenancyTubeConsumer(configuration);
    }

    private TubeSourceInfo generateTubeSourceInfo(String topic, String tid) {
        return new TubeSourceInfo(
                topic,
                masterAddress,
                null,
                new TDMsgCsvDeserializationInfo(tid, ',', true),
                new FieldInfo[0]);
    }

    @Test
    public void testCloneTubeTopicSubscriptionInfos() {
        final Map<String, TubeSubscriptionDescription> originalDescriptions = new HashMap<>();
        final TubeSubscriptionDescription description = new TubeSubscriptionDescription(topicAdded, masterAddress,
                null);

        description.addDataFlow(dataFlowId1, generateTubeSourceInfo(topicAdded, tid1));
        originalDescriptions.put(topicAdded, description);

        final Map<String, TubeSubscriptionDescription> cloned =
                MultiTopicTubeSourceFunction.cloneTubeTopicSubscriptionInfos(originalDescriptions);
        assertEquals(originalDescriptions, cloned);
        // verify it's deep copied
        assertNotSame(originalDescriptions.get(topicAdded), cloned.get(topicAdded));
        assertNotSame(originalDescriptions.get(topicAdded).getDataFlows(), cloned.get(topicAdded).getDataFlows());
    }

    @Test
    public void testProcessSourceEvent() {
        final TestingMultiTenancyTubeConsumer tubeConsumer = generateTubeConsumer();
        final MultiTopicTubeSourceFunction sourceFunction = new MultiTopicTubeSourceFunction(new Configuration());
        sourceFunction.setTubeConsumer(tubeConsumer);
        final Map<String, TubeSubscriptionDescription> descriptions = new HashMap<>();
        // add event1
        final SourceEvent addEvent1 = new SourceEvent(SourceEventType.ADDED, dataFlowId1, generateTubeSourceInfo(
                topicAdded, tid1));
        sourceFunction.processSourceEvent(addEvent1, descriptions);
        // add event should only change the preview
        assertTrue(tubeConsumer.addedTopics.isEmpty());
        assertTrue(tubeConsumer.updatedTopics.isEmpty());
        assertTrue(tubeConsumer.removedTopics.isEmpty());

        // add event1 again, should affect nothing
        sourceFunction.processSourceEvent(addEvent1, descriptions);

        // add event2
        final SourceEvent addEvent2 = new SourceEvent(SourceEventType.ADDED, dataFlowId2, generateTubeSourceInfo(
                topicAdded, tid2));
        sourceFunction.processSourceEvent(addEvent2, descriptions);
        // add event should only change the preview
        assertTrue(tubeConsumer.addedTopics.isEmpty());
        assertTrue(tubeConsumer.updatedTopics.isEmpty());
        assertTrue(tubeConsumer.removedTopics.isEmpty());
        assertEquals(1, descriptions.size());
        assertEquals(2, descriptions.get(topicAdded).getDataFlows().size());

        // remove event
        final TubeSubscriptionDescription existingDescription = new TubeSubscriptionDescription(topicRemoved,
                masterAddress, null);
        existingDescription.addDataFlow(dataFlowId3, generateTubeSourceInfo(topicRemoved, tid1));
        descriptions.put(topicRemoved, existingDescription);
        final SourceEvent removeEvent = new SourceEvent(SourceEventType.REMOVED, dataFlowId3,
                generateTubeSourceInfo(topicRemoved, tid1));
        sourceFunction.processSourceEvent(removeEvent, descriptions);
        assertTrue(tubeConsumer.addedTopics.isEmpty());
        assertTrue(tubeConsumer.updatedTopics.isEmpty());
        // remove event should be fired immediately
        assertEquals(1, tubeConsumer.removedTopics.size());
        assertEquals(topicRemoved, tubeConsumer.removedTopics.get(0));
        assertEquals(1, descriptions.size());
        assertEquals(2, descriptions.get(topicAdded).getDataFlows().size());
    }

    @Test
    public void testCommitTubeSubscriptionDescriptionPreview() {
        final TestingMultiTenancyTubeConsumer tubeConsumer = generateTubeConsumer();
        final MultiTopicTubeSourceFunction sourceFunction = new MultiTopicTubeSourceFunction(new Configuration());
        sourceFunction.setTubeConsumer(tubeConsumer);
        sourceFunction.setTubeTopicSubscriptions(Collections.emptyMap());
        final Map<String, TubeSubscriptionDescription> descriptionPreview = new HashMap<>();
        descriptionPreview.put(topicAdded,
                TubeSubscriptionDescription.generate(dataFlowId1, generateTubeSourceInfo(topicAdded, tid1)));

        sourceFunction.commitTubeSubscriptionDescriptionPreview(descriptionPreview);
        assertEquals(sourceFunction.getTubeTopicSubscriptions(), descriptionPreview);
        assertEquals(1, tubeConsumer.addedTopics.size());
        assertEquals(topicAdded, tubeConsumer.addedTopics.get(0).getTopic());
        assertEquals(masterAddress, tubeConsumer.addedTopics.get(0).getMasterAddress());
        assertNull(tubeConsumer.addedTopics.get(0).getConsumerGroup());
        assertEquals(Collections.singletonList(tid1), tubeConsumer.addedTopics.get(0).getTids());
    }

    private static class TestingMultiTenancyTubeConsumer extends MultiTenancyTubeConsumer {

        private final List<TubeSubscriptionDescription> addedTopics = new ArrayList<>();

        private final List<TubeSubscriptionDescription> updatedTopics = new ArrayList<>();

        private final List<String> removedTopics = new ArrayList<>();

        public TestingMultiTenancyTubeConsumer(Configuration configuration) {
            super(configuration, Collections.emptyMap(), 1, ignored -> {
            });
        }

        @Override
        public void addTopic(TubeSubscriptionDescription description) {
            addedTopics.add(description);
        }

        @Override
        public void updateTopic(TubeSubscriptionDescription description) {
            updatedTopics.add(description);
        }

        @Override
        public void removeTopic(String topic) {
            removedTopics.add(topic);
        }
    }
}