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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.sender.http.InLongHttpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multiple Instances Message Sender Factory
 *
 * Used to define the Multiple instance sender factory
 */
public class MsgSenderMultiFactory implements MsgSenderFactory {

    private static final AtomicLong refCounter = new AtomicLong(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final BaseMsgSenderFactory baseMsgSenderFactory;

    public MsgSenderMultiFactory() {
        this.baseMsgSenderFactory = new BaseMsgSenderFactory(
                this, "iMultiFact-" + refCounter.incrementAndGet());
        this.initialized.set(true);
    }

    @Override
    public void shutdownAll() throws ProxySdkException {
        if (!this.initialized.get()) {
            throw new ProxySdkException("Please initialize the factory first!");
        }
        this.baseMsgSenderFactory.close();
    }

    @Override
    public void removeClient(BaseSender msgSender) {
        if (msgSender == null
                || msgSender.getSenderFactory() == null
                || msgSender.getSenderFactory() != this) {
            return;
        }
        this.baseMsgSenderFactory.removeClient(msgSender);
    }

    @Override
    public int getMsgSenderCount() {
        return this.baseMsgSenderFactory.getMsgSenderCount();
    }

    @Override
    public InLongTcpMsgSender genTcpSenderByGroupId(
            TcpMsgSenderConfig configure) throws ProxySdkException {
        return genTcpSenderByGroupId(configure, null);
    }

    @Override
    public InLongTcpMsgSender genTcpSenderByGroupId(
            TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws ProxySdkException {
        if (!this.initialized.get()) {
            throw new ProxySdkException("Please initialize the factory first!");
        }
        return this.baseMsgSenderFactory.genTcpSenderByGroupId(configure, selfDefineFactory);
    }

    @Override
    public InLongTcpMsgSender genTcpSenderByClusterId(
            TcpMsgSenderConfig configure) throws ProxySdkException {
        return genTcpSenderByClusterId(configure, null);
    }

    @Override
    public InLongTcpMsgSender genTcpSenderByClusterId(
            TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws ProxySdkException {
        if (!this.initialized.get()) {
            throw new ProxySdkException("Please initialize the factory first!");
        }
        return this.baseMsgSenderFactory.genTcpSenderByClusterId(configure, selfDefineFactory);
    }

    @Override
    public InLongHttpMsgSender genHttpSenderByGroupId(HttpMsgSenderConfig configure) throws ProxySdkException {
        if (!this.initialized.get()) {
            throw new ProxySdkException("Please initialize the factory first!");
        }
        return this.baseMsgSenderFactory.genHttpSenderByGroupId(configure);
    }

    @Override
    public InLongHttpMsgSender genHttpSenderByClusterId(HttpMsgSenderConfig configure) throws ProxySdkException {
        if (!this.initialized.get()) {
            throw new ProxySdkException("Please initialize the factory first!");
        }
        return this.baseMsgSenderFactory.genHttpSenderByClusterId(configure);
    }
}
