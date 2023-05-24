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

package org.apache.inlong.dataproxy.source2.httpMsg;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.common.util.NetworkUtils;
import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.consts.AttrConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.consts.StatConstants;
import org.apache.inlong.dataproxy.source2.BaseSource;
import org.apache.inlong.dataproxy.utils.AddressUtils;
import org.apache.inlong.dataproxy.utils.DateTimeUtils;
import org.apache.inlong.dataproxy.utils.InLongMsgVer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;

import static io.netty.handler.codec.http.HttpUtil.is100ContinueExpected;

/**
 * HTTP Server message handler
 */
public class InLongHttpMsgHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final String hbSrvUrl = "/dataproxy/heartbeat";
    private static final String msgSrvUrl = "/dataproxy/message";

    private static final Logger logger = LoggerFactory.getLogger(InLongHttpMsgHandler.class);
    // log print count
    private static final LogCounter logCounter = new LogCounter(10, 100000, 30 * 1000);

    private final BaseSource source;

    /**
     * Constructor
     *
     * @param source AbstractSource
     */
    public InLongHttpMsgHandler(BaseSource source) {
        this.source = source;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        // check request decode result
        if (!req.decoderResult().isSuccess()) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_DECFAIL);
            sendErrorMsg(ctx, HttpResponseStatus.BAD_REQUEST, "Decode message failure!");
            return;
        }
        // check request method
        if (req.method() != HttpMethod.GET && req.method() != HttpMethod.POST) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_INVALIDMETHOD);
            sendErrorMsg(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, "Only support Get and Post methods");
            return;
        }
        // process 100-continue request
        if (is100ContinueExpected(req)) {
            ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
        }
        // get requested service
        String reqUri = req.uri();
        if (StringUtils.isBlank(reqUri)) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_BLANKURI);
            sendErrorMsg(ctx, HttpResponseStatus.BAD_REQUEST, "Uri is blank!");
            return;
        }
        try {
            reqUri = URLDecoder.decode(reqUri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            try {
                reqUri = URLDecoder.decode(reqUri, "ISO-8859-1");
            } catch (UnsupportedEncodingException e1) {
                source.fileMetricEventInc(StatConstants.EVENT_HTTP_URIDECFAIL);
                sendErrorMsg(ctx, HttpResponseStatus.BAD_REQUEST, "Decode uri failure!");
                return;
            }
        }
        // check requested service url
        if (!reqUri.startsWith(hbSrvUrl) || !reqUri.startsWith(msgSrvUrl)) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_INVALIDURI);
            sendErrorMsg(ctx, HttpResponseStatus.NOT_IMPLEMENTED, "Not supported uri!");
            return;
        }
        // get current time and clientIP
        long msgRcvTime = System.currentTimeMillis();
        String clientIp = AddressUtils.getChannelRemoteIP(ctx.channel());
        // check illegal ip
        if (ConfigManager.getInstance().needChkIllegalIP()
                && ConfigManager.getInstance().isIllegalIP(clientIp)) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_ILLEGAL_VISIT);
            sendResponse(ctx, DataProxyErrCode.ILLEGAL_VISIT_IP, true);
            return;
        }
        // check service status.
        if (source.isRejectService()) {
            source.fileMetricEventInc(StatConstants.EVENT_SERVICE_CLOSED);
            sendResponse(ctx, DataProxyErrCode.SERVICE_CLOSED, true);
            return;
        }
        // check sink service status
        if (!ConfigManager.getInstance().isMqClusterReady()) {
            source.fileMetricEventInc(StatConstants.EVENT_SERVICE_UNREADY);
            sendResponse(ctx, DataProxyErrCode.SINK_SERVICE_UNREADY, true);
            return;
        }
        // process hb service
        if (reqUri.startsWith(hbSrvUrl)) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_HB_SUCCESS);
            sendResponse(ctx, DataProxyErrCode.SUCCESS, checkClose(req));
            return;
        }
        // process message request
        processMessage(ctx, req, msgRcvTime, clientIp);
    }

    private boolean processMessage(ChannelHandlerContext ctx, FullHttpRequest req,
            long msgRcvTime, String clientIp) throws Exception {
        // get and check groupId
        HttpHeaders headers = req.headers();
        StringBuilder strBuff = new StringBuilder(512);
        String groupId = headers.get(AttributeConstants.GROUP_ID);
        if (StringUtils.isEmpty(groupId)) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_WITHOUTGROUPID);
            sendResponse(ctx, DataProxyErrCode.MISS_REQUIRED_GROUPID_ARGUMENT.getErrCode(),
                    strBuff.append("Field ").append(AttributeConstants.GROUP_ID)
                            .append(" must exist and not blank!").toString(),
                    checkClose(req));
            return false;
        }
        // get and check streamId
        String streamId = headers.get(AttributeConstants.STREAM_ID);
        if (StringUtils.isEmpty(streamId)) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_WITHOUTSTREAMID);
            sendResponse(ctx, DataProxyErrCode.MISS_REQUIRED_STREAMID_ARGUMENT.getErrCode(),
                    strBuff.append("Field ").append(AttributeConstants.STREAM_ID)
                            .append(" must exist and not blank!").toString(),
                    checkClose(req));
            return false;
        }
        // get and check topicName
        String topicName = ConfigManager.getInstance().getTopicName(groupId, streamId);
        if (StringUtils.isBlank(topicName)) {
            if (CommonConfigHolder.getInstance().isNoTopicAccept()) {
                source.fileMetricEventInc(StatConstants.EVENT_NOTOPIC);
                sendResponse(ctx, DataProxyErrCode.TOPIC_IS_BLANK.getErrCode(),
                        strBuff.append("Topic is null for ").append(AttributeConstants.GROUP_ID)
                                .append("(").append(groupId).append("),")
                                .append(AttributeConstants.STREAM_ID)
                                .append("(,").append(streamId).append(")").toString(),
                        checkClose(req));
                return false;
            }
            topicName = source.getDefTopic();
        }
        // get and check dt
        long dataTime = msgRcvTime;
        String dt = headers.get(AttributeConstants.DATA_TIME);
        if (StringUtils.isNotEmpty(dt)) {
            try {
                dataTime = Long.parseLong(dt);
            } catch (Throwable e) {
                //
            }
        }
        // get char set
        String charset = headers.get(AttrConstants.CHARSET);
        if (StringUtils.isBlank(charset)) {
            charset = AttrConstants.CHARSET;
        }
        // get and check body
        String body = headers.get(AttrConstants.BODY);
        if (StringUtils.isBlank(body)) {
            if (body == null) {
                source.fileMetricEventInc(StatConstants.EVENT_HTTP_NOBODY);
                sendResponse(ctx, DataProxyErrCode.MISS_REQUIRED_BODY_ARGUMENT.getErrCode(),
                        strBuff.append("Field ").append(AttrConstants.BODY)
                                .append(" is not exist!").toString(),
                        checkClose(req));
            } else {
                source.fileMetricEventInc(StatConstants.EVENT_HTTP_EMPTYBODY);
                sendResponse(ctx, DataProxyErrCode.EMPTY_MSG.getErrCode(),
                        strBuff.append("Field ").append(AttrConstants.BODY)
                                .append(" is Blank!").toString(),
                        checkClose(req));
            }
            return false;
        }
        if (body.length() > source.getMaxMsgLength()) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_BODYOVERMAXLEN);
            sendResponse(ctx, DataProxyErrCode.BODY_EXCEED_MAX_LEN.getErrCode(),
                    strBuff.append("Error msg, the body length(").append(body.length())
                            .append(") is bigger than allowed length(")
                            .append(source.getMaxMsgLength()).append(")").toString(),
                    checkClose(req));
            return false;
        }
        // get message count
        String strMsgCount = headers.get(AttributeConstants.MESSAGE_COUNT);
        int intMsgCnt = NumberUtils.toInt(strMsgCount, 1);
        strMsgCount = String.valueOf(intMsgCnt);
        // build message attributes
        InLongMsg inLongMsg = InLongMsg.newInLongMsg(source.isCompressed());
        strBuff.append("&groupId=").append(groupId)
                .append("&streamId=").append(streamId)
                .append("&dt=").append(dataTime)
                .append("&NodeIP=").append(clientIp)
                .append("&cnt=").append(strMsgCount)
                .append("&rt=").append(msgRcvTime)
                .append(AttributeConstants.SEPARATOR).append(AttributeConstants.MSG_RPT_TIME)
                .append(AttributeConstants.KEY_VALUE_SEPARATOR).append(msgRcvTime);
        inLongMsg.addMsg(strBuff.toString(), body.getBytes(charset));
        byte[] inlongMsgData = inLongMsg.buildArray();
        inLongMsg.reset();
        strBuff.delete(0, strBuff.length());
        // build flume event
        Map<String, String> eventHeaders = new HashMap<>();
        eventHeaders.put(AttributeConstants.GROUP_ID, groupId);
        eventHeaders.put(AttributeConstants.STREAM_ID, streamId);
        eventHeaders.put(ConfigConstants.TOPIC_KEY, topicName);
        eventHeaders.put(AttributeConstants.DATA_TIME, String.valueOf(dataTime));
        eventHeaders.put(ConfigConstants.REMOTE_IP_KEY, clientIp);
        eventHeaders.put(ConfigConstants.MSG_COUNTER_KEY, strMsgCount);
        eventHeaders.put(ConfigConstants.MSG_ENCODE_VER, InLongMsgVer.INLONG_V0.getName());
        eventHeaders.put(AttributeConstants.RCV_TIME, String.valueOf(msgRcvTime));
        Event event = EventBuilder.withBody(inlongMsgData, eventHeaders);
        String msgProcType = "b2b";
        // build metric data item
        dataTime = dataTime / 1000 / 60 / 10;
        dataTime = dataTime * 1000 * 60 * 10;
        strBuff.append("http").append(AttrConstants.SEP_HASHTAG).append(topicName)
                .append(AttrConstants.SEP_HASHTAG).append(streamId)
                .append(AttrConstants.SEP_HASHTAG).append(clientIp)
                .append(AttrConstants.SEP_HASHTAG).append(NetworkUtils.getLocalIp())
                .append(AttrConstants.SEP_HASHTAG).append(msgProcType)
                .append(AttrConstants.SEP_HASHTAG).append(DateTimeUtils.ms2yyyyMMddHHmm(dataTime))
                .append(AttrConstants.SEP_HASHTAG).append(DateTimeUtils.ms2yyyyMMddHHmm(msgRcvTime));
        try {
            source.getChannelProcessor().processEvent(event);
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_POST_SUCCESS);
            source.fileMetricRecordAdd(strBuff.toString(), intMsgCnt, 1, body.length(), 0);
            strBuff.delete(0, strBuff.length());
            source.addMetric(true, event.getBody().length, event);
            sendResponse(ctx, DataProxyErrCode.SUCCESS, false);
            return true;
        } catch (ChannelException ex) {
            source.fileMetricEventInc(StatConstants.EVENT_HTTP_POST_DROPPED);
            source.fileMetricRecordAdd(strBuff.toString(), 0, 0, 0, intMsgCnt);
            source.addMetric(false, event.getBody().length, event);
            strBuff.delete(0, strBuff.length());
            sendResponse(ctx, DataProxyErrCode.UNKNOWN_ERROR.getErrCode(),
                    strBuff.append("Put event to channel failure: ").append(ex.getMessage())
                            .toString(),
                    false);
            if (logCounter.shouldPrint()) {
                logger.error("Error write event to channel, data will discard.", ex);
            }
            return false;
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String clientIp = AddressUtils.getChannelRemoteIP(ctx.channel());
        logger.error("Http process client={} error, cause:{}, msg:{}",
                cause, clientIp, cause.getMessage());
        sendErrorMsg(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Process message failure: " + cause.getMessage());
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (IdleStateEvent.class.isAssignableFrom(evt.getClass())) {
            ctx.close();
        }
    }

    private boolean checkClose(FullHttpRequest req) {
        String connStatus = req.headers().get("Connection");
        return !StringUtils.isBlank(connStatus) && "close".equalsIgnoreCase(connStatus);
    }

    private void sendErrorMsg(ChannelHandlerContext ctx, HttpResponseStatus status, String errMsg) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer("Failure: " + status + ", "
                        + errMsg + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ctx.writeAndFlush(response).addListener(new SendResultListener(true));
    }

    private void sendResponse(ChannelHandlerContext ctx, DataProxyErrCode errCodeObj, boolean isClose) {
        sendResponse(ctx, errCodeObj.getErrCode(), errCodeObj.getErrMsg(), isClose);

    }

    private void sendResponse(ChannelHandlerContext ctx, int errCode, String errMsg, boolean isClose) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=utf-8");
        StringBuilder builder =
                new StringBuilder().append("{\"code\":\"").append(errCode)
                        .append("\",\"msg\":\"").append(errMsg).append("\"}");
        ByteBuf buffer = Unpooled.copiedBuffer(builder.toString(), CharsetUtil.UTF_8);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
        response.content().writeBytes(buffer);
        buffer.release();
        ctx.writeAndFlush(response).addListener(new SendResultListener(isClose));
    }

    private class SendResultListener implements ChannelFutureListener {

        private final boolean isClose;

        public SendResultListener(boolean isClose) {
            this.isClose = isClose;
        }

        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
            if (!channelFuture.isSuccess()) {
                Throwable throwable = channelFuture.cause();
                String clientIp = AddressUtils.getChannelRemoteIP(channelFuture.channel());
                if (logCounter.shouldPrint()) {
                    logger.error("Http return response to client {} failed, exception:{}, errmsg:{}",
                            clientIp, throwable, throwable.getLocalizedMessage());
                }
                channelFuture.channel().close();
            }
            if (isClose) {
                channelFuture.channel().close();
            }
        }
    }
}
