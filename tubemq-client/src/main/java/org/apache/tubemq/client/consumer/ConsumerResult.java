/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.client.consumer;

import java.util.ArrayList;
import java.util.List;
import org.apache.tubemq.client.common.PeerInfo;
import org.apache.tubemq.corebase.Message;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.cluster.Partition;


public class ConsumerResult {
    private boolean success = false;
    private int errCode = TBaseConstants.META_VALUE_UNDEFINED;
    private String errMsg = "";
    private String topicName = "";
    private PeerInfo peerInfo = new PeerInfo();
    private String confirmContext = "";
    private List<Message> messageList = new ArrayList<Message>();


    public ConsumerResult(int errCode, String errMsg) {
        this.success = false;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public ConsumerResult(FetchContext taskContext) {
        this.success = taskContext.isSuccess();
        this.errCode = taskContext.getErrCode();
        this.errMsg = taskContext.getErrMsg();
        this.topicName = taskContext.getPartition().getTopic();
        peerInfo.setMsgSourceInfo(taskContext.getPartition(), taskContext.getCurrOffset());
        if (this.success) {
            this.messageList = taskContext.getMessageList();
            this.confirmContext = taskContext.getConfirmContext();
        }
    }

    public ConsumerResult(boolean isSuccess, int errCode, String errMsg,
                          String topicName, Partition partition, long currOffset) {
        this.success = isSuccess;
        this.errCode = errCode;
        this.errMsg = errMsg;
        this.topicName = topicName;
        this.peerInfo.setMsgSourceInfo(partition, currOffset);
    }

    public boolean isSuccess() {
        return success;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getTopicName() {
        return topicName;
    }

    public PeerInfo getPeerInfo() {
        return peerInfo;
    }

    public String getPartitionKey() {
        return peerInfo.getPartitionKey();
    }

    public String getErrMsg() {
        return errMsg;
    }

    public final String getConfirmContext() {
        return confirmContext;
    }

    public long getCurrOffset() {
        return peerInfo.getCurrOffset();
    }

    public final List<Message> getMessageList() {
        return messageList;
    }
}
