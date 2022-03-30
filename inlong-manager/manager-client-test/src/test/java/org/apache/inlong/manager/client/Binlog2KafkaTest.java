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
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.DataSeparator;
import org.apache.inlong.manager.client.api.FlinkSortBaseConf;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.InlongGroupContext;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.PulsarBaseConf;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.sink.KafkaSink;
import org.apache.inlong.manager.client.api.source.MySQLBinlogSource;
import org.apache.inlong.manager.common.enums.MQType;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Binlog2KafkaTest {

    // Manager web url
    public static String SERVICE_URL = "127.0.0.1:8083";
    // Inlong user && passwd
    public static DefaultAuthentication INLONG_AUTH = new DefaultAuthentication("admin", "inlong");
    // Inlong group name
    public static String GROUP_NAME = "{group.name}";
    // Inlong stream name
    public static String STREAM_NAME = "{stream.name}";
    // Flink cluster url
    public static String FLINK_URL = "{flink.cluster.url}";
    // Pulsar cluster admin url
    public static String PULSAR_ADMIN_URL = "{pulsar.admin.url}";
    // Pulsar cluster service url
    public static String PULSAR_SERVICE_URL = "{pulsar.service.url}";
    // Pulsar tenant
    public static String tenant = "{pulsar.tenant}";
    // Pulsar topic
    public static String topic = "{pulsar.topic}";

    @Test
    public void testCreateGroupForKafka() throws Exception {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setWriteTimeout(10);
        configuration.setReadTimeout(10);
        configuration.setConnectTimeout(10);
        configuration.setTimeUnit(TimeUnit.SECONDS);
        configuration.setAuthentication(INLONG_AUTH);
        InlongClient inlongClient = InlongClient.create(SERVICE_URL, configuration);
        InlongGroupConf groupConf = createGroupConf();
        try {
            InlongGroup group = inlongClient.forGroup(groupConf);
            InlongStreamConf streamConf = createStreamConf();
            InlongStreamBuilder streamBuilder = group.createStream(streamConf);
            streamBuilder.source(createMysqlSource());
            streamBuilder.sink(createKafkaSink());
            streamBuilder.initOrUpdate();
            // start group
            InlongGroupContext inlongGroupContext = group.init();
            log.info("{}", inlongGroupContext);
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
        configuration.setAuthentication(INLONG_AUTH);
        InlongClient inlongClient = InlongClient.create(SERVICE_URL, configuration);
        InlongGroupConf groupConf = createGroupConf();
        try {
            InlongGroup group = inlongClient.forGroup(groupConf);
            InlongGroupContext groupContext = group.delete(true);
            log.info("{}", groupContext);
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
        configuration.setAuthentication(INLONG_AUTH);
        InlongClient inlongClient = InlongClient.create(SERVICE_URL, configuration);
        InlongGroupConf groupConf = createGroupConf();
        try {
            InlongGroup group = inlongClient.forGroup(groupConf);
            InlongGroupContext groupContext = group.restart(true);
            log.info("{}", groupContext);
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
        configuration.setAuthentication(INLONG_AUTH);
        InlongClient inlongClient = InlongClient.create(SERVICE_URL, configuration);
        InlongGroupConf groupConf = createGroupConf();
        try {
            InlongGroup group = inlongClient.forGroup(groupConf);
            InlongGroupContext groupContext = group.suspend(true);
            log.info("{}", groupContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private InlongGroupConf createGroupConf() {
        InlongGroupConf inlongGroupConf = new InlongGroupConf();
        inlongGroupConf.setGroupName(GROUP_NAME);
        inlongGroupConf.setDescription(GROUP_NAME);
        inlongGroupConf.setProxyClusterId(1);
        //pulsar conf
        PulsarBaseConf pulsarBaseConf = new PulsarBaseConf();
        pulsarBaseConf.setType(MQType.PULSAR);
        inlongGroupConf.setMqBaseConf(pulsarBaseConf);
        pulsarBaseConf.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        pulsarBaseConf.setPulsarAdminUrl(PULSAR_ADMIN_URL);
        pulsarBaseConf.setNamespace("public");
        pulsarBaseConf.setEnableCreateResource(false);
        pulsarBaseConf.setTenant(tenant);
        //flink conf
        FlinkSortBaseConf sortBaseConf = new FlinkSortBaseConf();
        inlongGroupConf.setSortBaseConf(sortBaseConf);
        sortBaseConf.setServiceUrl(FLINK_URL);
        Map<String, String> map = new HashMap<>(16);
        sortBaseConf.setProperties(map);
        //enable zk
        inlongGroupConf.setZookeeperEnabled(false);
        inlongGroupConf.setDailyRecords(10000000L);
        inlongGroupConf.setPeakRecords(100000L);
        inlongGroupConf.setMaxLength(10000);
        return inlongGroupConf;
    }

    private InlongStreamConf createStreamConf() {
        InlongStreamConf streamConf = new InlongStreamConf();
        streamConf.setName(STREAM_NAME);
        streamConf.setDescription(STREAM_NAME);
        streamConf.setCharset(StandardCharsets.UTF_8);
        streamConf.setDataSeparator(DataSeparator.VERTICAL_BAR);
        // true if you need strictly order for data
        streamConf.setStrictlyOrdered(true);
        streamConf.setTopic(topic);
        return streamConf;
    }

    public MySQLBinlogSource createMysqlSource() {
        MySQLBinlogSource mySQLBinlogSource = new MySQLBinlogSource();
        mySQLBinlogSource.setDbNames(Arrays.asList("{db.name}"));
        mySQLBinlogSource.setHostname("{db.url}");
        mySQLBinlogSource.setAuthentication(new DefaultAuthentication("root", "inlong"));
        mySQLBinlogSource.setSourceName("{mysql.source.name}");
        mySQLBinlogSource.setAllMigration(true);
        return mySQLBinlogSource;
    }

    private KafkaSink createKafkaSink() {
        KafkaSink kafkaSink = new KafkaSink();
        kafkaSink.setDataFormat(DataFormat.CANAL);
        kafkaSink.setAddress("{kafka.bootstrap}");
        kafkaSink.setTopicName("{kafka.topic}");
        kafkaSink.setNeedCreated(false);
        kafkaSink.setSinkName("{kafka.sink.name}");
        Map<String, Object> properties = new HashMap<>();
        //Not needed if kafka cluster is not set
        properties.put("transaction.timeout.ms", 9000000);
        kafkaSink.setProperties(properties);
        return kafkaSink;
    }
}
