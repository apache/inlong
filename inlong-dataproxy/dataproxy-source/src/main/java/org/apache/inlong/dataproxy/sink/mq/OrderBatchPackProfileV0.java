/**
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

package org.apache.inlong.dataproxy.sink.mq;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.inlong.dataproxy.base.OrderEvent;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.source.MsgType;
import org.apache.inlong.dataproxy.utils.MessageUtils;
import org.apache.inlong.sdk.commons.protocol.InlongId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.netty.buffer.ByteBuf;

/**
 * SimpleBatchPackProfileV0
 * 
 */
public class OrderBatchPackProfileV0 extends BatchPackProfile {

    public static final Logger LOG = LoggerFactory.getLogger(OrderBatchPackProfileV0.class);

    private OrderEvent orderProfile;

    /**
     * Constructor
     * @param uid
     * @param inlongGroupId
     * @param inlongStreamId
     * @param dispatchTime
     */
    public OrderBatchPackProfileV0(String uid, String inlongGroupId, String inlongStreamId, long dispatchTime) {
        super(uid, inlongGroupId, inlongStreamId, dispatchTime);
    }

    /**
     * create
     * @param event
     * @return
     */
    public static OrderBatchPackProfileV0 create(OrderEvent event) {
        Map<String, String> headers = event.getHeaders();
        String inlongGroupId = headers.get(AttributeConstants.GROUP_ID);;
        String inlongStreamId = headers.get(AttributeConstants.STREAM_ID);
        String uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
        long msgTime = NumberUtils.toLong(headers.get(AttributeConstants.DATA_TIME), System.currentTimeMillis());
        long dispatchTime = msgTime - msgTime % MINUTE_MS;
        OrderBatchPackProfileV0 profile = new OrderBatchPackProfileV0(uid, inlongGroupId, inlongStreamId,
                dispatchTime);
        profile.setCount(1);
        profile.setSize(event.getBody().length);
        profile.orderProfile = event;
        return profile;
    }

    /**
     * get event
     * @return the event
     */
    public OrderEvent getOrderProfile() {
        return orderProfile;
    }

    /**
     * ackOrder
     */
    public void ackOrder() {
        String sequenceId = orderProfile.getHeaders().get(AttributeConstants.UNIQ_ID);
        if ("false".equals(orderProfile.getHeaders().get(AttributeConstants.MESSAGE_IS_ACK))) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not need to rsp message: seqId = {}, inlongGroupId = {}, inlongStreamId = {}",
                        sequenceId, this.getInlongGroupId(), this.getInlongStreamId());
            }
            return;
        }
        if (orderProfile.getCtx() != null && orderProfile.getCtx().channel().isActive()) {
            orderProfile.getCtx().channel().eventLoop().execute(() -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("order message rsp: seqId = {}, inlongGroupId = {}, inlongStreamId = {}", sequenceId,
                            this.getInlongGroupId(), this.getInlongStreamId());
                }
                ByteBuf binBuffer = MessageUtils.getResponsePackage("", MsgType.MSG_BIN_MULTI_BODY, sequenceId);
                orderProfile.getCtx().writeAndFlush(binBuffer);
            });
        }
    }
}
