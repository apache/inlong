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

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.sources.extend.DefaultExtendedHandler;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MqttSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttSource.class);

    private MqttClient client;
    private LinkedBlockingQueue<DefaultMessage> mqttMessagesQueue;
    private String serverURI;

    private String topic;

    private int qos;

    private String clientId;

    MqttConnectOptions options;

    public MqttSource() {
    }

    @Override
    protected String getThreadName() {
        return "mqtt-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("MqttSource init: {}", profile.toJsonStr());
            mqttMessagesQueue = new LinkedBlockingQueue<>(profile.getInt(TaskConstants.TASK_MQTT_QUEUE_SIZE, 1000));
            serverURI = profile.get(TaskConstants.TASK_MQTT_SERVER_URI);
            instanceId = profile.getInstanceId();
            topic = profile.get(TaskConstants.TASK_MQTT_TOPIC);
            qos = profile.getInt(TaskConstants.TASK_MQTT_QOS, 1);
            clientId = profile.get(TaskConstants.TASK_MQTT_CLIENT_ID_PREFIX, "mqtt_client") + "_" + UUID.randomUUID();
            initConnectOptions(profile);
            mqttConnect();
        } catch (Exception e) {
            stopRunning();
            throw new FileException("error init stream for {}" + topic, e);
        }
    }

    private void initConnectOptions(InstanceProfile profile) {
        options = new MqttConnectOptions();
        options.setCleanSession(profile.getBoolean(TaskConstants.TASK_MQTT_CLEAN_SESSION, false));
        options.setConnectionTimeout(profile.getInt(TaskConstants.TASK_MQTT_CONNECTION_TIMEOUT, 10));
        options.setKeepAliveInterval(profile.getInt(TaskConstants.TASK_MQTT_KEEPALIVE_INTERVAL, 20));
        options.setUserName(profile.get(TaskConstants.TASK_MQTT_USERNAME, ""));
        options.setPassword(profile.get(TaskConstants.TASK_MQTT_PASSWORD, "").toCharArray());
        options.setAutomaticReconnect(profile.getBoolean(TaskConstants.TASK_MQTT_AUTOMATIC_RECONNECT, true));
        options.setMqttVersion(
                profile.getInt(TaskConstants.TASK_MQTT_VERSION, MqttConnectOptions.MQTT_VERSION_DEFAULT));
    }

    private void mqttConnect() {
        try {
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
                    mqttMessagesQueue.offer(new DefaultMessage(recordValue, headerMap));

                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            client.connect(options);
            client.subscribe(topic, qos);
            LOGGER.info("the mqtt subscribe topic is [{}], qos is [{}]", topic, qos);
        } catch (Exception e) {
            LOGGER.error("init mqtt client error {}. jobId:{},serverURI:{},clientId:{}", e, instanceId, serverURI,
                    clientId);
        }
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

    private void disconnect() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            LOGGER.error("disconnect mqtt client error {}. jobId:{},serverURI:{},clientId:{}", e, instanceId, serverURI,
                    clientId);
        }
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("mqtt topic is {}", topic);
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        try {
            int size = 0;
            while (size < BATCH_READ_LINE_TOTAL_LEN) {
                Message msg = mqttMessagesQueue.poll(1, TimeUnit.SECONDS);
                if (msg != null) {
                    SourceData sourceData = new SourceData(msg.getBody(), "0L");
                    size += sourceData.getData().length;
                    dataList.add(sourceData);
                } else {
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("poll {} data from mqtt queue interrupted.", instanceId);
        }
        return dataList;
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        LOGGER.info("release mqtt source");
        if (client.isConnected()) {
            disconnect();
        }
    }

    @Override
    public boolean sourceExist() {
        return true;
    }
}
