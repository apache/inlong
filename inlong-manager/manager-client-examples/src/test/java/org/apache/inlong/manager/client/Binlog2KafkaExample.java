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

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.common.enums.DataSeparator;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupContext;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.sink.KafkaSink;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.shiro.util.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test class for binlog to kafka.
 */
@Slf4j
public class Binlog2KafkaExample extends BaseExample {

    @Test
    public void testCreateGroupForKafka() {
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
            streamBuilder.source(createMysqlSource());
            streamBuilder.sink(createKafkaSink());
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
            InlongGroupContext groupContext = group.delete(true);
            Assert.notNull(groupContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSuspendGroup() {
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
            InlongGroupContext groupContext = group.suspend(true);
            Assert.notNull(groupContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRestartGroup() {
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
            InlongGroupContext groupContext = group.restart(true);
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

    private MySQLBinlogSource createMysqlSource() {
        MySQLBinlogSource mySQLBinlogSource = new MySQLBinlogSource();
        mySQLBinlogSource.setDbNames(Collections.singletonList("{db.name}"));
        mySQLBinlogSource.setHostname("{db.url}");
        mySQLBinlogSource.setAuthentication(new DefaultAuthentication("root", "inlong"));
        mySQLBinlogSource.setSourceName("{mysql.source.name}");
        mySQLBinlogSource.setAllMigration(true);
        return mySQLBinlogSource;
    }

    private KafkaSink createKafkaSink() {
        KafkaSink kafkaSink = new KafkaSink();
        kafkaSink.setDataFormat(DataFormat.CANAL);
        kafkaSink.setBootstrapServers("{kafka.bootstrap}");
        kafkaSink.setTopicName("{kafka.topic}");
        kafkaSink.setNeedCreated(false);
        kafkaSink.setSinkName("{kafka.sink.name}");
        Map<String, Object> properties = new HashMap<>();
        // Not needed if kafka cluster is not set
        properties.put("transaction.timeout.ms", 9000000);
        kafkaSink.setProperties(properties);
        return kafkaSink;
    }
}
