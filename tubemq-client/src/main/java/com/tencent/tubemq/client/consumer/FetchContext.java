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

package com.tencent.tubemq.client.consumer;

import com.tencent.tubemq.corebase.Message;
import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TErrCodeConstants;
import com.tencent.tubemq.corebase.cluster.Partition;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchContext {
    private static final Logger logger = LoggerFactory.getLogger(FetchContext.class);
    private Partition partition;
    private long usedToken;
    private boolean lastConsumed = false;
    private boolean success = false;
    private int errCode = 0;
    private String errMsg = "";
    private long currOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private String confirmContext = "";
    private List<Message> messageList = new ArrayList<Message>();

    public FetchContext(PartitionSelectResult selectResult) {
        this.partition = selectResult.getPartition();
        this.usedToken = selectResult.getUsedToken();
        this.lastConsumed = selectResult.isLastPackConsumed();
    }

    public void setFailProcessResult(int errCode, String errMsg) {
        this.success = false;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    public void setSuccessProcessResult(long currOffset,
                                        String confirmContext,
                                        List<Message> messageList) {
        this.success = true;
        this.errCode = TErrCodeConstants.SUCCESS;
        this.errMsg = "Ok!";
        if (currOffset >= 0) {
            this.currOffset = currOffset;
        }
        this.confirmContext = confirmContext;
        this.messageList = messageList;
    }

    public Partition getPartition() {
        return partition;
    }

    public long getUsedToken() {
        return usedToken;
    }

    public boolean isLastConsumed() {
        return lastConsumed;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public List<Message> getMessageList() {
        return messageList;
    }

    public long getCurrOffset() {
        return currOffset;
    }

    public String getConfirmContext() {
        return confirmContext;
    }
}
