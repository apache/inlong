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

package org.apache.inlong.manager.client.api.impl;

import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.sink.ClickHouseSink;
import org.apache.inlong.manager.client.api.sink.HiveSink;
import org.apache.inlong.manager.client.api.sink.KafkaSink;
import org.apache.inlong.manager.client.api.source.KafkaSource;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.client.api.transform.MultiDependencyTransform;
import org.apache.inlong.manager.client.api.transform.SingleDependencyTransform;
import org.apache.inlong.manager.client.api.util.GsonUtils;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.pojo.stream.StreamTransform;
import org.apache.inlong.manager.common.pojo.transform.filter.FilterDefinition;
import org.apache.inlong.manager.common.pojo.transform.filter.FilterDefinition.FilterStrategy;
import org.apache.inlong.manager.common.pojo.transform.joiner.JoinerDefinition;
import org.apache.inlong.manager.common.pojo.transform.joiner.JoinerDefinition.JoinMode;
import org.apache.inlong.manager.common.pojo.transform.splitter.SplitterDefinition;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for creat inlong stream.
 */
public class InlongStreamImplTest {

    @Test
    public void testCreatePipeline() {
        InlongStream inlongStream = new InlongStreamImpl("group", "stream", null);
        // add stream source
        KafkaSource kafkaSource = new KafkaSource();
        kafkaSource.setSourceName("A");
        MySQLBinlogSource mySQLBinlogSource = new MySQLBinlogSource();
        mySQLBinlogSource.setSourceName("B");
        inlongStream.addSource(kafkaSource);
        inlongStream.addSource(mySQLBinlogSource);
        // add stream sink
        ClickHouseSink clickHouseSink = new ClickHouseSink();
        clickHouseSink.setSinkName("E");
        HiveSink hiveSink = new HiveSink();
        hiveSink.setSinkName("F");
        KafkaSink kafkaSink1 = new KafkaSink();
        kafkaSink1.setSinkName("I");
        KafkaSink kafkaSink2 = new KafkaSink();
        kafkaSink2.setSinkName("M");
        inlongStream.addSink(clickHouseSink);
        inlongStream.addSink(hiveSink);
        inlongStream.addSink(kafkaSink1);
        inlongStream.addSink(kafkaSink2);
        // add stream transform
        StreamTransform multiDependencyTransform = new MultiDependencyTransform(
                "C",
                new JoinerDefinition(kafkaSource, mySQLBinlogSource, Lists.newArrayList(), Lists.newArrayList(),
                        JoinMode.INNER_JOIN),
                "A", "B");
        StreamTransform singleDependencyTransform1 = new SingleDependencyTransform(
                "D", new FilterDefinition(FilterStrategy.REMOVE, Lists.newArrayList()), "C", "E", "F"
        );

        StreamTransform singleDependencyTransform2 = new SingleDependencyTransform(
                "G", new SplitterDefinition(Lists.newArrayList()), "C", "I"
        );
        inlongStream.addTransform(multiDependencyTransform);
        inlongStream.addTransform(singleDependencyTransform1);
        inlongStream.addTransform(singleDependencyTransform2);
        StreamPipeline streamPipeline = inlongStream.createPipeline();
        String pipelineView = GsonUtils.toJson(streamPipeline);
        Assert.assertTrue(pipelineView.contains("{\"inputNodes\":[\"C\"],\"outputNodes\":[\"D\",\"G\"]"));
        Assert.assertTrue(pipelineView.contains("{\"inputNodes\":[\"D\"],\"outputNodes\":[\"E\",\"F\"]}"));
    }
}
