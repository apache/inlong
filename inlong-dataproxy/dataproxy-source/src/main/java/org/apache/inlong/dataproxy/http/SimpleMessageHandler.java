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

package org.apache.inlong.dataproxy.http;

import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.Event;
import org.apache.inlong.commons.monitor.CounterGroup;
import org.apache.inlong.commons.monitor.CounterGroupExt;
import org.apache.inlong.commons.monitor.MonitorIndex;
import org.apache.inlong.commons.monitor.MonitorIndexExt;
import org.apache.inlong.commons.monitor.StatConstants;
import org.apache.inlong.commons.msg.TDMsg1;
import org.apache.inlong.commons.util.NetworkUtils;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.source.ServiceDecoder;
import org.apache.inlong.dataproxy.http.exception.MessageProcessException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMessageHandler
        implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMessageHandler.class);
    private static final ConfigManager configManager = ConfigManager.getInstance();

    private final CounterGroup counterGroup;
    private final CounterGroupExt counterGroupExt;

    private static final String SEPARATOR = "#";
    private final boolean isNewMetricOn = true;
    private final MonitorIndex monitorIndex = new MonitorIndex("Source", 60, 300000);
    private final MonitorIndexExt monitorIndexExt =
            new MonitorIndexExt("DataProxy_monitors#http", 60, 100000);

    @SuppressWarnings("unused")
    private int maxMsgLength;
    private long logCounter = 0L;
    private long channelTrace = 0L;

    private static final ThreadLocal<SimpleDateFormat> dateFormator =
            new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat("yyyyMMddHHmm");
                }
            };

    private static final String DEFAULT_REMOTE_IDC_VALUE = "0";
    private final ChannelProcessor processor;

    public SimpleMessageHandler(ChannelProcessor processor, CounterGroup counterGroup,
            CounterGroupExt counterGroupExt, ServiceDecoder decoder) {
        this.processor = processor;
        this.counterGroup = counterGroup;
        this.counterGroupExt = counterGroupExt;
        init();
    }

    @Override
    public void init() {
    }

    @Override
    public void destroy() {

    }

    @Override
    public void processMessage(Context context) throws MessageProcessException {
        String topic = "test";
        String topicValue = topic;
        String attr = "m=0";
        StringBuffer newAttrBuffer = new StringBuffer(attr);

        String groupId = (String) context.get(HttpSourceConstants.GROUP_ID);
        String streamId = (String) context.get(HttpSourceConstants.INTERFACE_ID);
        String dt = (String) context.get(HttpSourceConstants.DATA_TIME);

        String value = getTopic(groupId, streamId);
        if (null != value && !"".equals(value)) {
            topicValue = value.trim();
        }

        String mxValue = configManager.getMxProperties().get(groupId);
        if (null != mxValue) {
            newAttrBuffer = new StringBuffer(mxValue.trim());
        }

        newAttrBuffer.append("&groupId=").append(groupId).append("&streamId=").append(streamId)
                .append("&dt=").append(dt);
        HttpServletRequest request =
                (HttpServletRequest) context.get(HttpSourceConstants.HTTP_REQUEST);
        String strRemoteIP = request.getRemoteAddr();
        newAttrBuffer.append("&NodeIP=").append(strRemoteIP);
        String msgCount = request.getParameter(HttpSourceConstants.MESSAGE_COUNT);
        if (msgCount == null || "".equals(msgCount)) {
            msgCount = "1";
        }

        TDMsg1 tdMsg = TDMsg1.newTDMsg(true);
        String charset = (String) context.get(HttpSourceConstants.CHARSET);
        if (charset == null || "".equals(charset)) {
            charset = "UTF-8";
        }
        String body = (String) context.get(HttpSourceConstants.BODY);
        try {
            tdMsg.addMsg(newAttrBuffer.toString(), body.getBytes(charset));
        } catch (UnsupportedEncodingException e) {
            throw new MessageProcessException(e);
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(HttpSourceConstants.DATA_TIME, dt);
        headers.put(ConfigConstants.TOPIC_KEY, topicValue);
        headers.put(AttributeConstants.INTERFACE_ID, streamId);
        headers.put(ConfigConstants.REMOTE_IP_KEY, strRemoteIP);
        headers.put(ConfigConstants.REMOTE_IDC_KEY, DEFAULT_REMOTE_IDC_VALUE);
        headers.put(ConfigConstants.MSG_COUNTER_KEY, msgCount);
        byte[] data = tdMsg.buildArray();
        headers.put(ConfigConstants.TOTAL_LEN, String.valueOf(data.length));
        String pkgTime = dateFormator.get().format(tdMsg.getCreatetime());
        headers.put(ConfigConstants.PKG_TIME_KEY, pkgTime);
        Event event = EventBuilder.withBody(data, headers);

        String counterExtKey = topicValue + "#" + 0 + "#" + strRemoteIP + "#time#" + pkgTime;
        counterGroupExt.addAndGet(counterExtKey, Long.valueOf(msgCount));

        long dtten = 0;
        try {
            dtten = Long.parseLong(dt);
        } catch (NumberFormatException e1) {
            throw new MessageProcessException(new Throwable(
                    "attribute dt=" + dt + " has error," + " detail is: " + newAttrBuffer));
        }

        dtten = dtten / 1000 / 60 / 10;
        dtten = dtten * 1000 * 60 * 10;

        StringBuilder newbase = new StringBuilder();
        newbase.append("http").append(SEPARATOR).append(topicValue).append(SEPARATOR)
                .append(streamId).append(SEPARATOR).append(strRemoteIP).append(SEPARATOR)
                .append(NetworkUtils.getLocalIp()).append(SEPARATOR)
                .append(new SimpleDateFormat("yyyyMMddHHmm").format(dtten)).append(SEPARATOR)
                .append(pkgTime);

        if (isNewMetricOn) {
            monitorIndex
                    .addAndGet(new String(newbase), Integer.parseInt(msgCount), 1, data.length, 0);
        }
        tdMsg.reset();

        long beginTime = System.currentTimeMillis();
        try {
            processor.processEvent(event);
            counterGroup.incrementAndGet(StatConstants.EVENT_SUCCESS);
            monitorIndexExt.incrementAndGet("EVENT_SUCCESS");

        } catch (ChannelException ex) {
            counterGroup.incrementAndGet(StatConstants.EVENT_DROPPED);
            monitorIndexExt.incrementAndGet("EVENT_DROPPED");
            if (isNewMetricOn) {
                monitorIndex.addAndGet(new String(newbase), 0, 0, 0, Integer.parseInt(msgCount));
            }

            logCounter++;
            if (logCounter == 1 || logCounter % 1000 == 0) {
                LOG.error("Error writting to channel,and will retry after 1s,ex={},"
                        + "logCounter = {}, spend time={} ms", new Object[]{ex.toString(), logCounter,
                        System.currentTimeMillis() - beginTime});
                if (logCounter > Long.MAX_VALUE - 10) {
                    logCounter = 0;
                    LOG.info("logCounter will reverse.");
                }
            }
            throw ex;
        }
        channelTrace++;
        if (channelTrace % 600000 == 0) {
            LOG.info("processor.processEvent spend time={} ms",
                    System.currentTimeMillis() - beginTime);
        }
        if (channelTrace > Long.MAX_VALUE - 10) {
            channelTrace = 0;
            LOG.info("channelTrace will reverse.");
        }
    }

    @Override
    public void configure(org.apache.flume.Context context) {
    }

    private Map<String, String> getAttributeMap(String attrs) {
        if (null != attrs && !"".equals(attrs)) {
            try {
                Splitter.MapSplitter mySplitter =
                        Splitter.on("&").trimResults().withKeyValueSeparator("=");
                return mySplitter.split(attrs);
            } catch (Exception e) {
                LOG.error("fail to fillTopicKeyandAttr,attr:{}", attrs);
                LOG.error("error!", e);
            }
        }
        return null;
    }

    private Map<String, String> loadProperties(String fileName) {
        HashMap<String, String> map = new HashMap<String, String>();
        if (null == fileName) {
            LOG.error("fail to loadTopics, null == fileName.");
            return map;
        }
        InputStream inStream = null;
        try {
            URL url = getClass().getClassLoader().getResource(fileName);
            inStream = url != null ? url.openStream() : null;
            if (inStream == null) {
                LOG.error("InputStream {} is null!", fileName);
                return map;
            }
            Properties props = new Properties();
            props.load(inStream);
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                map.put((String) entry.getKey(), (String) entry.getValue());
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("fail to loadPropery, file ={}, and e= {}", fileName, e);
        } catch (Exception e) {
            LOG.error("fail to loadProperty, file ={}, and e= {}", fileName, e);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    LOG.error("fail to loadTopics, inStream.close ,and e= {}", fileName, e);
                }
            }
        }
        return map;
    }

    private String getTopic(String groupId, String streamId) {
        String topic = null;
        if (StringUtils.isNotEmpty(groupId)) {
            if (StringUtils.isNotEmpty(streamId)) {
                topic = configManager.getTopicProperties().get(groupId + "/" + streamId);
            }
            if (StringUtils.isEmpty(topic)) {
                topic = configManager.getTopicProperties().get(groupId);
            }
        }
        LOG.debug("Get topic by groupId/streamId = {}, topic = {}", groupId + "/" + streamId,
                topic);
        return topic;
    }

}
