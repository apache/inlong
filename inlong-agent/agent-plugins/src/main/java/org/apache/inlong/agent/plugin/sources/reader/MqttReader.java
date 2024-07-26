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

package org.apache.inlong.agent.plugin.sources.reader;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.reader.file.AbstractReader;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttReader extends AbstractReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttReader.class);

    private boolean finished = false;

    private boolean destroyed = false;

    private MqttClient client;

    private MqttConnectOptions options;

    private String serverURI;
    private String userName;
    private String password;
    private String topic;
    private int qos;
    private boolean cleanSession = false;
    private boolean automaticReconnect = true;
    private InstanceProfile jobProfile;
    private String instanceId;
    private String clientId;
    private int mqttVersion = MqttConnectOptions.MQTT_VERSION_DEFAULT;

    private LinkedBlockingQueue<DefaultMessage> mqttMessagesQueue;

    public MqttReader(String topic) {
        this.topic = topic;
    }

    /**
     * set global params value
     *
     * @param jobConf
     */
    private void setGlobalParamsValue(InstanceProfile jobConf) {
        mqttMessagesQueue = new LinkedBlockingQueue<>(jobConf.getInt(TaskConstants.TASK_MQTT_QUEUE_SIZE, 1000));
        instanceId = jobConf.getInstanceId();
        userName = jobConf.get(TaskConstants.TASK_MQTT_USERNAME);
        password = jobConf.get(TaskConstants.TASK_MQTT_PASSWORD);
        serverURI = jobConf.get(TaskConstants.TASK_MQTT_SERVER_URI);

        // topic = jobConf.get(TaskConstants.TASK_MQTT_TOPIC);
        clientId = jobConf.get(TaskConstants.TASK_MQTT_CLIENT_ID_PREFIX, "mqtt_client") + "_" + UUID.randomUUID();
        cleanSession = jobConf.getBoolean(TaskConstants.TASK_MQTT_CLEAN_SESSION, false);
        automaticReconnect = jobConf.getBoolean(TaskConstants.TASK_MQTT_AUTOMATIC_RECONNECT, true);
        qos = jobConf.getInt(TaskConstants.TASK_MQTT_QOS, 1);
        mqttVersion = jobConf.getInt(TaskConstants.TASK_MQTT_VERSION, MqttConnectOptions.MQTT_VERSION_DEFAULT);
        options = new MqttConnectOptions();
        options.setCleanSession(cleanSession);
        options.setConnectionTimeout(jobConf.getInt(TaskConstants.TASK_MQTT_CONNECTION_TIMEOUT, 10));
        options.setKeepAliveInterval(jobConf.getInt(TaskConstants.TASK_MQTT_KEEPALIVE_INTERVAL, 20));
        options.setUserName(userName);
        options.setPassword(password.toCharArray());
        options.setAutomaticReconnect(automaticReconnect);
        options.setMqttVersion(mqttVersion);
    }

    /**
     * connect to MQTT Broker
     */
    private void connect() {

        try {
            synchronized (MqttReader.class) {
                client = new MqttClient(serverURI, clientId, new MemoryPersistence());
                client.setCallback(new MqttCallback() {

                    @Override
                    public void connectionLost(Throwable cause) {
                        LOGGER.error("the mqtt jobId:{}, serverURI:{}, connection lost, {} ", instanceId,
                                serverURI, cause);
                        reconnect();
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        Map<String, String> headerMap = new HashMap<>();
                        headerMap.put("record.topic", topic);
                        headerMap.put("record.messageId", String.valueOf(message.getId()));
                        headerMap.put("record.qos", String.valueOf(message.getQos()));
                        byte[] recordValue = message.getPayload();
                        mqttMessagesQueue.put(new DefaultMessage(recordValue, headerMap));

                        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                                System.currentTimeMillis(), 1, recordValue.length);

                        readerMetric.pluginReadSuccessCount.incrementAndGet();
                        readerMetric.pluginReadCount.incrementAndGet();
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                    }
                });
                client.connect(options);
                Thread.sleep(1000);
                client.subscribe(topic, qos);
            }
            LOGGER.info("the mqtt subscribe topic is [{}], qos is [{}]", topic, qos);
        } catch (Exception e) {
            LOGGER.error("init mqtt client error {}. jobId:{},serverURI:{},clientId:{}", e, instanceId, serverURI,
                    clientId);
        }
    }

    @Override
    public void init(InstanceProfile jobConf) {
        super.init(jobConf);
        jobProfile = jobConf;
        LOGGER.info("init mqtt reader with jobConf {}", jobConf.toJsonStr());
        setGlobalParamsValue(jobConf);
        connect();
    }

    private void reconnect() {
        if (!client.isConnected()) {
            try {
                client.connect(options);
                LOGGER.info("the mqtt client reconnect success. jobId:{}, serverURI:{}, clientId:{}", instanceId,
                        serverURI, clientId);
            } catch (Exception e) {
                LOGGER.error("reconnect mqtt client error {}. jobId:{}, serverURI:{}, clientId:{}", e, instanceId,
                        serverURI, clientId);
            }
        }
    }

    @Override
    public Message read() {
        if (!mqttMessagesQueue.isEmpty()) {
            return getMqttMessage();
        } else {
            return null;
        }
    }

    private DefaultMessage getMqttMessage() {
        return mqttMessagesQueue.poll();
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public String getReadSource() {
        return instanceId;
    }

    @Override
    public void setReadTimeout(long mill) {
    }

    @Override
    public void setWaitMillisecond(long millis) {
    }

    @Override
    public String getSnapshot() {
        return StringUtils.EMPTY;
    }

    @Override
    public void finishRead() {
        finished = true;
    }

    @Override
    public boolean isSourceExist() {
        return true;
    }

    private void disconnect() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            LOGGER.error("disconnect mqtt client error {}. jobId:{},serverURI:{},clientId:{}", e, instanceId, serverURI,
                    clientId);
        }
    }

    public void setReadSource(String instanceId) {
        this.instanceId = instanceId;
    }

    @Override
    public void destroy() {
        synchronized (this) {
            if (!destroyed) {
                disconnect();
                destroyed = true;
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final String serverURI = "tcp://broker.hivemq.com:1883";
        final String username = "test";
        final String password = "test";
        final String qos = "0";
        final String clientIdPrefix = "mqtt_client";
        final String groupId = "group01";
        final String streamId = "stream01";
        MqttReader reader = new MqttReader("test/inlongtest");
        InstanceProfile jobconf = new InstanceProfile();
        jobconf.set(CommonConstants.PROXY_INLONG_GROUP_ID, groupId);
        jobconf.set(CommonConstants.PROXY_INLONG_STREAM_ID, streamId);
        jobconf.set(TaskConstants.TASK_MQTT_USERNAME, username);
        jobconf.set(TaskConstants.TASK_MQTT_PASSWORD, password);
        jobconf.set(TaskConstants.TASK_MQTT_SERVER_URI, serverURI);
        jobconf.set(TaskConstants.TASK_MQTT_QOS, qos);
        jobconf.set(TaskConstants.TASK_MQTT_CLIENT_ID_PREFIX, clientIdPrefix);
        jobconf.setInstanceId("instanceId");
        jobconf.set(TaskConstants.TASK_MQTT_QUEUE_SIZE, "1000");
        reader.init(jobconf);
    }
}
