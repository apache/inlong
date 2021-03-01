/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tubemq.bus.source;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashSet;
import java.util.Set;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.apache.tubemq.bus.channel.FailoverChannelProcessor;
import org.apache.tubemq.bus.channel.FailoverChannelProcessorHolder;
import org.apache.tubemq.bus.monitor.CounterGroup;
import org.apache.tubemq.bus.monitor.CounterGroupExt;
import org.apache.tubemq.bus.monitor.StatConstants;
import org.apache.tubemq.bus.monitor.StatRunner;
import org.apache.tubemq.bus.util.ConfStringUtils;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final String CONNECTIONS = "connections";
    private static final Logger logger = LoggerFactory.getLogger(BaseSource.class);
    protected Context context;
    protected int port;
    protected String host = null;
    protected String msgFactoryName;
    protected String serviceDecoderName;
    protected String messageHandlerName;
    protected int maxMsgLength;
    protected boolean isCompressed;
    protected String topic;
    protected String attr;
    protected boolean filterEmptyMsg;
    protected int pkgTimeoutSec;
    protected CounterGroup counterGroup;
    protected CounterGroupExt counterGroupExt;
    protected ChannelGroup allChannels;
    protected int maxConnections = Integer.MAX_VALUE;
    protected boolean customProcessor = false;
    private int statIntervalSec;
    private StatRunner statRunner;
    private Thread statThread;
    private boolean isOldMetircOn = true;

    public BaseSource() {
        super();
        counterGroup = new CounterGroup();
        counterGroupExt = new CounterGroupExt();
        allChannels = new DefaultChannelGroup();
    }

    @Override
    public synchronized void start() {
        if (statIntervalSec > 0 && isOldMetircOn) {
            Set<String> moniterNames = new HashSet<String>();
            moniterNames.add(StatConstants.EVENT_SUCCESS);
            moniterNames.add(StatConstants.EVENT_DROPPED);
            moniterNames.add(StatConstants.EVENT_EMPTY);
            moniterNames.add(StatConstants.EVENT_OTHEREXP);
            moniterNames.add(StatConstants.EVENT_INVALID);
            statRunner =
                    new StatRunner(getName(), counterGroup, counterGroupExt, statIntervalSec,
                            moniterNames);
            statThread = new Thread(statRunner);
            statThread.setName("Thread-Stat-" + this.getName());
            statThread.start();
        }

        if (customProcessor) {
            ChannelSelector selector = getChannelProcessor().getSelector();
            FailoverChannelProcessor newProcessor = new FailoverChannelProcessor(selector);
            newProcessor.configure(this.context);
            setChannelProcessor(newProcessor);
            FailoverChannelProcessorHolder.setChannelProcessor(newProcessor);
        }

        super.start();
    }

    @Override
    public synchronized void stop() {
        if (statIntervalSec > 0) {
            try {
                if (statRunner != null) {
                    statRunner.shutDown();
                }
                if (statThread != null) {
                    statThread.interrupt();
                    statThread.join();
                }
            } catch (InterruptedException e) {
                logger.warn("statrunner interrupted");
            }
        }

        super.stop();
    }

    @Override
    public void configure(Context context) {
        logger.info(context.toString());
        this.context = context;
        port = context.getInteger(ConfigConstants.CONFIG_PORT);

        host = context.getString(ConfigConstants.CONFIG_HOST, "0.0.0.0");
        isOldMetircOn = context.getBoolean("old-metric-on", true);

//    isNewMetricOn = context.getBoolean("new-metric-on", true);
        Configurables.ensureRequiredNonNull(context, ConfigConstants.CONFIG_PORT);

        checkArgument(ConfStringUtils.isValidIp(host), "ip config not valid");
        checkArgument(ConfStringUtils.isValidPort(port), "port config not valid");

        msgFactoryName =
                context.getString(ConfigConstants.MSG_FACTORY_NAME,
                        "org.apache.flume.source.ServerMessageFactory");
        msgFactoryName = msgFactoryName.trim();
        checkArgument(!msgFactoryName.isEmpty(), "msgFactoryName is empty");

        serviceDecoderName =
                context.getString(ConfigConstants.SERVICE_PROCESSOR_NAME,
                        "org.apache.flume.source.DefaultServiceDecoder");

        serviceDecoderName = serviceDecoderName.trim();
        checkArgument(!serviceDecoderName.isEmpty(), "serviceProcessorName is empty");

        messageHandlerName =
                context.getString(ConfigConstants.MESSAGE_HANDLER_NAME,
                        "org.apache.flume.source.ServerMessageHandler");
        messageHandlerName = messageHandlerName.trim();
        checkArgument(!messageHandlerName.isEmpty(), "messageHandlerName is empty");

        maxMsgLength = context.getInteger(ConfigConstants.MAX_MSG_LENGTH, 1024 * 64);
        checkArgument(
                (maxMsgLength >= 4 && maxMsgLength <= ConfigConstants.MSG_MAX_LENGTH_BYTES),
                "maxMsgLength must be >= 4 and <= 65536");

        isCompressed = context.getBoolean(ConfigConstants.MSG_COMPRESSED, true);

        topic = context.getString(ConfigConstants.TOPIC);
        attr = context.getString(ConfigConstants.ATTR);
        Configurables.ensureRequiredNonNull(context, ConfigConstants.TOPIC, ConfigConstants.ATTR);

        topic = topic.trim();
        checkArgument(!topic.isEmpty(), "topic is empty");
        attr = attr.trim();
        checkArgument(!attr.isEmpty(), "attr is empty");

        filterEmptyMsg = context.getBoolean(ConfigConstants.FILTER_EMPTY_MSG, false);

        statIntervalSec = context.getInteger(ConfigConstants.STAT_INTERVAL_SEC, 60);
        checkArgument((statIntervalSec >= 0), "statIntervalSec must be >= 0");

        pkgTimeoutSec = context.getInteger(ConfigConstants.PACKAGE_TIMEOUT_SEC, 3);

        try {
            maxConnections = context.getInteger(CONNECTIONS, 5000);
        } catch (NumberFormatException e) {
            logger.warn("BaseSource\'s \"connections\" property  specify an integer value.",
                    context.getString(CONNECTIONS));
        }

        this.customProcessor = context.getBoolean(ConfigConstants.CUSTOM_CHANNEL_PROCESSOR, false);
    }

    public Context getContext() {
        return context;
    }
}
