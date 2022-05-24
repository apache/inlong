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

package org.apache.inlong.manager.client;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.common.enums.DataSeparator;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupContext;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.sink.HiveSink;
import org.apache.inlong.manager.client.api.source.AgentFileSource;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.enums.FileFormat;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.stream.SinkField;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.shiro.util.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test class for file to hive.
 */
@Slf4j
public class File2HiveExample extends BaseExample {

    @Test
    public void testCreateGroupForHive() {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setWriteTimeout(10);
        configuration.setReadTimeout(10);
        configuration.setConnectTimeout(10);
        configuration.setTimeUnit(TimeUnit.SECONDS);
        configuration.setAuthentication(super.getInlongAuth());
        InlongClient inlongClient = InlongClient.create(super.getServiceUrl(), configuration);

        InlongGroupInfo groupInfo = super.createGroupInfo();
        try {
            InlongGroup group = inlongClient.forGroup(groupInfo);
            InlongStreamConf streamConf = createStreamConf();
            InlongStreamBuilder streamBuilder = group.createStream(streamConf);
            streamBuilder.fields(createStreamFields());
            streamBuilder.source(createAgentFileSource());
            streamBuilder.sink(createHiveSink());
            streamBuilder.initOrUpdate();
            // start group
            InlongGroupContext inlongGroupContext = group.init();
            Assert.notNull(inlongGroupContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStopGroup() {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setWriteTimeout(10);
        configuration.setReadTimeout(10);
        configuration.setConnectTimeout(10);
        configuration.setTimeUnit(TimeUnit.SECONDS);
        configuration.setAuthentication(super.getInlongAuth());
        InlongClient inlongClient = InlongClient.create(super.getServiceUrl(), configuration);
        InlongGroupInfo groupInfo = createGroupInfo();
        try {
            InlongGroup group = inlongClient.forGroup(groupInfo);
            InlongGroupContext groupContext = group.delete();
            Assert.notNull(groupContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private InlongStreamConf createStreamConf() {
        InlongStreamConf streamConf = new InlongStreamConf();
        streamConf.setName(super.getStreamId());
        streamConf.setCharset(StandardCharsets.UTF_8);
        streamConf.setDataSeparator(DataSeparator.VERTICAL_BAR);
        // true if you need strictly order for data
        streamConf.setStrictlyOrdered(true);
        streamConf.setMqResource(super.getTopic());
        return streamConf;
    }

    private AgentFileSource createAgentFileSource() {
        AgentFileSource agentFileSource = new AgentFileSource();
        agentFileSource.setAgentIp("{agent.ip}");
        agentFileSource.setPattern("/a/b/*.txt");
        agentFileSource.setTimeOffset("-1h");
        return agentFileSource;
    }

    private HiveSink createHiveSink() {
        HiveSink hiveSink = new HiveSink();
        hiveSink.setDbName("{db.name}");
        hiveSink.setJdbcUrl("jdbc:hive2://{ip:port}");
        hiveSink.setAuthentication(new DefaultAuthentication("hive", "hive"));
        hiveSink.setCharset(StandardCharsets.UTF_8);
        hiveSink.setFileFormat(FileFormat.TextFile);
        hiveSink.setDataSeparator(DataSeparator.VERTICAL_BAR);
        hiveSink.setDataPath("hdfs://{ip:port}/usr/hive/warehouse/{db.name}");

        List<SinkField> fields = new ArrayList<>();
        SinkField field1 = new SinkField(0, FieldType.INT, "age", FieldType.INT, "age");
        SinkField field2 = new SinkField(1, FieldType.STRING, "name", FieldType.STRING, "name");
        fields.add(field1);
        fields.add(field2);
        hiveSink.setSinkFields(fields);
        hiveSink.setTableName("{table.name}");
        hiveSink.setSinkName("{hive.sink.name}");
        return hiveSink;
    }

    private List<StreamField> createStreamFields() {
        List<StreamField> streamFieldList = Lists.newArrayList();
        streamFieldList.add(new StreamField(0, FieldType.STRING, "name", null, null));
        streamFieldList.add(new StreamField(1, FieldType.INT, "age", null, null));
        return streamFieldList;
    }
}
