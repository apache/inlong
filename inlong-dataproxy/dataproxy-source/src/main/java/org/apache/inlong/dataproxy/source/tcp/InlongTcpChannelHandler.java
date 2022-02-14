/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.source.tcp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItem;
import org.apache.inlong.dataproxy.metrics.audit.AuditUtils;
import org.apache.inlong.dataproxy.source.SourceContext;
import org.apache.inlong.sdk.commons.protocol.EventUtils;
import org.apache.inlong.sdk.commons.protocol.ProxyEvent;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.ResponseInfo;
import org.apache.inlong.sdk.commons.protocol.ProxySdk.ResultCode;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InlongTcpChannelHandler
 */
public class InlongTcpChannelHandler extends SimpleChannelHandler {

    public static final Logger LOG = LoggerFactory.getLogger(InlongTcpChannelHandler.class);
    public static final int LENGTH_PARAM_OFFSET = 0;
    public static final int LENGTH_PARAM_LENGTH = 4;
    public static final int VERSION_PARAM_OFFSET = 4;
    public static final int VERSION_PARAM_LENGTH = 2;
    public static final int BODY_PARAM_OFFSET = 6;

    public static final int VERSION_1 = 1;

    private SourceContext sourceContext;

    /**
     * Constructor
     * 
     * @param sourceContext
     */
    public InlongTcpChannelHandler(SourceContext sourceContext) {
        this.sourceContext = sourceContext;
    }

    /**
     * channelOpen
     * 
     * @param  ctx
     * @param  e
     * @throws Exception
     */
    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (sourceContext.getAllChannels().size() - 1 >= sourceContext.getMaxConnections()) {
            LOG.warn("refuse to connect , and connections=" + (sourceContext.getAllChannels().size() - 1)
                    + ", maxConnections="
                    + sourceContext.getMaxConnections() + ",channel is " + e.getChannel());
            e.getChannel().disconnect();
            e.getChannel().close();
        }
    }

    /**
     * messageReceived
     * 
     * @param  ctx
     * @param  e
     * @throws Exception
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        LOG.debug("message received");
        if (e == null) {
            LOG.error("get null messageevent, just skip");
            this.addMetric(false, 0, null);
            return;
        }
        ChannelBuffer cb = ((ChannelBuffer) e.getMessage());
        int readableLength = cb.readableBytes();
        if (readableLength == 0) {
            LOG.warn("skip empty msg.");
            cb.clear();
            this.addMetric(false, 0, null);
            return;
        }
        if (readableLength > LENGTH_PARAM_LENGTH + VERSION_PARAM_LENGTH + sourceContext.getMaxMsgLength()) {
            this.addMetric(false, 0, null);
            throw new Exception("err msg, MSG_MAX_LENGTH_BYTES "
                    + "< readableLength, and readableLength=" + readableLength + ", and MSG_MAX_LENGTH_BYTES="
                    + sourceContext.getMaxMsgLength());
        }
        // save index, reset it if buffer is not satisfied.
        cb.markReaderIndex();
        int totalPackLength = cb.readInt();
        cb.resetReaderIndex();
        if (readableLength < totalPackLength + LENGTH_PARAM_LENGTH) {
            // reset index.
            this.addMetric(false, 0, null);
            throw new Exception("err msg, channel buffer is not satisfied, and  readableLength="
                    + readableLength + ", and totalPackLength=" + totalPackLength);
        }

        // read version
        int version = cb.readShort();
        switch (version) {
            case VERSION_1 :
                // decode version 1
                int bodyLength = totalPackLength - VERSION_PARAM_LENGTH;
                decodeVersion1(cb, bodyLength, e);
                break;
            default :
                this.addMetric(false, 0, null);
                throw new Exception("err version, unknown version:" + version);
        }
    }

    private void decodeVersion1(ChannelBuffer cb, int bodyLength, MessageEvent e) throws Exception {
        // read bytes
        byte[] msgBytes = new byte[bodyLength];
        cb.readBytes(msgBytes);
        // decode
        List<ProxyEvent> events = EventUtils.decodeSdkPack(msgBytes, 0, bodyLength);
        // topic
        for (ProxyEvent event : events) {
            String uid = event.getUid();
            String topic = sourceContext.getIdHolder().getTopic(uid);
            if (topic != null) {
                event.setTopic(topic);
            }
            // put to channel
            try {
                sourceContext.getSource().getChannelProcessor().processEvent(event);
                this.addMetric(true, event.getBody().length, event);
            } catch (Throwable ex) {
                LOG.error("Process Controller Event error can't write event to channel.", ex);
                this.addMetric(false, event.getBody().length, event);
                this.responsePackage(ResultCode.ERR_REJECT, e);
                return;
            }
        }
        this.responsePackage(ResultCode.SUCCUSS, e);
    }

    /**
     * addMetric
     * 
     * @param result
     * @param size
     * @param event
     */
    private void addMetric(boolean result, long size, Event event) {
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, sourceContext.getProxyClusterId());
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_ID, sourceContext.getSourceId());
        dimensions.put(DataProxyMetricItem.KEY_SOURCE_DATA_ID, sourceContext.getSourceDataId());
        DataProxyMetricItem.fillInlongId(event, dimensions);
        DataProxyMetricItem.fillAuditFormatTime(event, dimensions);
        DataProxyMetricItem metricItem = this.sourceContext.getMetricItemSet().findMetricItem(dimensions);
        if (result) {
            metricItem.readSuccessCount.incrementAndGet();
            metricItem.readSuccessSize.addAndGet(size);
            AuditUtils.add(AuditUtils.AUDIT_ID_DATAPROXY_READ_SUCCESS, event);
        } else {
            metricItem.readFailCount.incrementAndGet();
            metricItem.readFailSize.addAndGet(size);
        }
    }

    /**
     * responsePackage
     * 
     * @param  code
     * @param  e
     * @throws Exception
     */
    private void responsePackage(ResultCode code, MessageEvent e)
            throws Exception {
        ResponseInfo.Builder builder = ResponseInfo.newBuilder();
        builder.setResult(code);

        // encode
        byte[] responseBytes = builder.build().toByteArray();
        //
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(responseBytes);

        Channel remoteChannel = e.getChannel();
        if (remoteChannel.isWritable()) {
            remoteChannel.write(buffer, e.getRemoteAddress());
        } else {
            LOG.warn(
                    "the send buffer2 is full, so disconnect it!please check remote client"
                            + "; Connection info:" + remoteChannel);
            throw new Exception(
                    "the send buffer2 is full,so disconnect it!please check remote client, Connection info:"
                            + remoteChannel);
        }
    }

    /**
     * exceptionCaught
     * 
     * @param  ctx
     * @param  e
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.error("exception caught", e.getCause());
        super.exceptionCaught(ctx, e);
        if (e.getChannel() != null) {
            try {
                e.getChannel().disconnect();
                e.getChannel().close();
            } catch (Exception ex) {
                LOG.error("Close connection error!", ex);
            }
            sourceContext.getAllChannels().remove(e.getChannel());
        }
    }

    /**
     * channelClosed
     * 
     * @param  ctx
     * @param  e
     * @throws Exception
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        LOG.debug("Connection to {} disconnected.", e.getChannel());
        super.channelClosed(ctx, e);
        try {
            e.getChannel().disconnect();
            e.getChannel().close();
        } catch (Exception ex) {
            //
        }
        sourceContext.getAllChannels().remove(e.getChannel());
    }
}
