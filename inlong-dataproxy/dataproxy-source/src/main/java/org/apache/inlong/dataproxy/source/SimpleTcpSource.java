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

package org.apache.inlong.dataproxy.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.apache.inlong.commons.config.metrics.MetricRegister;
import org.apache.inlong.dataproxy.base.NamedThreadFactory;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItemSet;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Simple tcp source
 *
 */
public class SimpleTcpSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(SimpleTcpSource.class);
    public static ArrayList<String> blacklist = new ArrayList<String>();
    private static final String blacklistFilePath = "blacklist.properties";
    private static long propsLastModified;
    private static final String CONNECTIONS = "connections";

    protected int maxConnections = Integer.MAX_VALUE;
    private ServerBootstrap serverBootstrap = null;
    protected ChannelGroup allChannels;
    protected int port;
    protected String host = null;
    protected String msgFactoryName;
    protected String serviceDecoderName;
    protected String messageHandlerName;
    protected int maxMsgLength;
    protected boolean isCompressed;
    private CheckBlackListThread checkBlackListThread;
    private int maxThreads = 32;

    private boolean tcpNoDelay = true;
    private boolean keepAlive = true;
    private int receiveBufferSize;
    private int highWaterMark;
    private int sendBufferSize;
    private int trafficClass;

    protected String topic;
    protected String attr;
    protected boolean filterEmptyMsg;

    private Channel nettyChannel = null;
    //
    private DataProxyMetricItemSet metricItemSet;

    public SimpleTcpSource() {
        super();
        allChannels = new DefaultChannelGroup();
    }

    /**
     * check black list
     * @param blacklist
     * @param allChannels
     */
    public static void checkBlackList(ArrayList blacklist, ChannelGroup allChannels) {
        if (blacklist != null) {
            Iterator<Channel> it = allChannels.iterator();
            while (it.hasNext()) {
                Channel channel = it.next();
                String strRemoteIP = null;
                SocketAddress remoteSocketAddress = channel.getRemoteAddress();
                if (null != remoteSocketAddress) {
                    strRemoteIP = remoteSocketAddress.toString();
                    try {
                        strRemoteIP = strRemoteIP.substring(1, strRemoteIP.indexOf(':'));
                    } catch (Exception ee) {
                        logger.warn("fail to get the remote IP, and strIP={},remoteSocketAddress={}", strRemoteIP,
                                remoteSocketAddress);
                    }
                }
                if (strRemoteIP != null && blacklist.contains(strRemoteIP)) {
                    logger.error(strRemoteIP + " is in blacklist, so disconnect it !");
                    channel.disconnect();
                    channel.close();
                    allChannels.remove(channel);
                }
            }
        }
    }

    private ArrayList<String> load(String fileName) {
        ArrayList<String> arrayList = new ArrayList<String>();
        if (fileName == null) {
            logger.error("fail to loadProperties, filename is null");
            return arrayList;
        }
        FileReader reader = null;
        BufferedReader br = null;
        try {
            reader = new FileReader("conf/" + fileName);
            br = new BufferedReader(reader);
            String line = null;
            while ((line = br.readLine()) != null) {
                arrayList.add(line);
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("fail to loadPropery, file ={}, and e= {}", fileName, e);
        } catch (Exception e) {
            logger.error("fail to loadProperty, file ={}, and e= {}", fileName, e);
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(br);
        }
        return arrayList;
    }

    private class CheckBlackListThread extends Thread {
        private boolean shutdown = false;

        public void shutdouwn() {
            shutdown = true;
        }

        @Override
        public void run() {
            logger.info("CheckBlackListThread thread {} start.", Thread.currentThread().getName());
            while (!shutdown) {
                try {
                    File blacklistFile = new File("conf/" + blacklistFilePath);
                    if (blacklistFile.lastModified() > propsLastModified) {
                        blacklist = load(blacklistFilePath);
                        propsLastModified = blacklistFile.lastModified();
                        SimpleDateFormat formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        logger.info("blacklist.properties:{}\n{}",
                                formator.format(new Date(blacklistFile.lastModified())), blacklist);
                    }
                    Thread.sleep(5 * 1000);
                    checkBlackList(blacklist, allChannels);
                } catch (InterruptedException e) {
                    logger.info("ConfigReloader thread exit!");
                    return;
                } catch (Throwable t) {
                    logger.error("ConfigReloader thread error!", t);
                }
            }
        }
    }

    @Override
    public synchronized void start() {
        logger.info("start " + this.getName());
        this.metricItemSet = new DataProxyMetricItemSet(this.getName());
        MetricRegister.register(metricItemSet);
        checkBlackListThread = new CheckBlackListThread();
        checkBlackListThread.start();
        super.start();

        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(
                                new NamedThreadFactory("tcpSource-nettyBoss-threadGroup")), 1,
                        Executors.newCachedThreadPool(
                                new NamedThreadFactory("tcpSource-nettyWorker-threadGroup")), maxThreads);
        logger.info("Set max workers : {} ;", maxThreads);
        ChannelPipelineFactory fac = null;

        serverBootstrap = new ServerBootstrap(factory);
        serverBootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
        serverBootstrap.setOption("child.keepAlive", keepAlive);
        serverBootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
        serverBootstrap.setOption("child.sendBufferSize", sendBufferSize);
        serverBootstrap.setOption("child.trafficClass", trafficClass);
        serverBootstrap.setOption("child.writeBufferHighWaterMark", highWaterMark);
        logger.info("load msgFactory=" + msgFactoryName
                + " and serviceDecoderName=" + serviceDecoderName);
        try {

            ServiceDecoder serviceDecoder =
                    (ServiceDecoder) Class.forName(serviceDecoderName).newInstance();

            Class<? extends ChannelPipelineFactory> clazz =
                    (Class<? extends ChannelPipelineFactory>) Class.forName(msgFactoryName);

            Constructor ctor =
                    clazz.getConstructor(AbstractSource.class, ChannelGroup.class,
                            String.class, ServiceDecoder.class, String.class,
                            Integer.class, String.class, String.class, Boolean.class,
                            Integer.class, Boolean.class, String.class);

            logger.info("Using channel processor:{}", this.getClass().getName());
            fac = (ChannelPipelineFactory) ctor.newInstance(this, allChannels,
                    "tcp", serviceDecoder, messageHandlerName,
                    maxMsgLength, topic, attr, filterEmptyMsg, maxConnections, isCompressed, this.getName());

        } catch (Exception e) {
            logger.error("Simple Tcp Source start error, fail to construct ChannelPipelineFactory with name {}, ex {}",
                    msgFactoryName, e);
            stop();
            throw new FlumeException(e.getMessage());
        }

        serverBootstrap.setPipelineFactory(fac);

        try {
            if (host == null) {
                nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
            } else {
                nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
            }
        } catch (Exception e) {
            logger.error("Simple TCP Source error bind host {} port {},program will exit!", host, port);
            System.exit(-1);
        }

        allChannels.add(nettyChannel);

        logger.info("Simple TCP Source started at host {}, port {}", host, port);

    }

    @Override
    public synchronized void stop() {
        logger.info("[STOP SOURCE]{} stopping...", super.getName());
        checkBlackListThread.shutdouwn();
        if (allChannels != null && !allChannels.isEmpty()) {
            try {
                allChannels.unbind().awaitUninterruptibly();
                allChannels.close().awaitUninterruptibly();
            } catch (Exception e) {
                logger.warn("Simple TCP Source netty server stop ex", e);
            } finally {
                allChannels.clear();
                // allChannels = null;
            }
        }

        if (serverBootstrap != null) {
            try {

                serverBootstrap.releaseExternalResources();
            } catch (Exception e) {
                logger.warn("Simple TCP Source serverBootstrap stop ex ", e);
            } finally {
                serverBootstrap = null;
            }
        }

        super.stop();
        logger.info("[STOP SOURCE]{} stopped", super.getName());
    }

    @Override
    public void configure(Context context) {
        logger.info("context is {}", context);
        port = context.getInteger(ConfigConstants.CONFIG_PORT);
        host = context.getString(ConfigConstants.CONFIG_HOST, "0.0.0.0");

        tcpNoDelay = context.getBoolean(ConfigConstants.TCP_NO_DELAY, true);

        keepAlive = context.getBoolean(ConfigConstants.KEEP_ALIVE, true);
        highWaterMark = context.getInteger(ConfigConstants.HIGH_WATER_MARK, 64 * 1024);
        receiveBufferSize = context.getInteger(ConfigConstants.RECEIVE_BUFFER_SIZE, 1024 * 64);
        if (receiveBufferSize > 16 * 1024 * 1024) {
            receiveBufferSize = 16 * 1024 * 1024;
        }
        Preconditions.checkArgument(receiveBufferSize > 0, "receiveBufferSize must be > 0");

        sendBufferSize = context.getInteger(ConfigConstants.SEND_BUFFER_SIZE, 1024 * 64);
        if (sendBufferSize > 16 * 1024 * 1024) {
            sendBufferSize = 16 * 1024 * 1024;
        }
        Preconditions.checkArgument(sendBufferSize > 0, "sendBufferSize must be > 0");

        trafficClass = context.getInteger(ConfigConstants.TRAFFIC_CLASS, 0);
        Preconditions.checkArgument((trafficClass == 0 || trafficClass == 96),
                "trafficClass must be == 0 or == 96");

        try {
            maxThreads = context.getInteger(ConfigConstants.MAX_THREADS, 32);
        } catch (NumberFormatException e) {
            logger.warn("Simple TCP Source max-threads property must specify an integer value. {}",
                    context.getString(ConfigConstants.MAX_THREADS));
        }

        try {
            maxConnections = context.getInteger(CONNECTIONS, 5000);
        } catch (NumberFormatException e) {
            logger.warn("BaseSource\'s \"connections\" property must specify an integer value.",
                    context.getString(CONNECTIONS));
        }

        topic = context.getString(ConfigConstants.TOPIC);
        attr = context.getString(ConfigConstants.ATTR);
        Configurables.ensureRequiredNonNull(context, ConfigConstants.TOPIC, ConfigConstants.ATTR);

        topic = topic.trim();
        Preconditions.checkArgument(!topic.isEmpty(), "topic is empty");
        attr = attr.trim();
        Preconditions.checkArgument(!attr.isEmpty(), "attr is empty");

        filterEmptyMsg = context.getBoolean(ConfigConstants.FILTER_EMPTY_MSG, false);

        msgFactoryName =
                context.getString(ConfigConstants.MSG_FACTORY_NAME,
                        "org.apache.inlong.dataproxy.source.ServerMessageFactory");
        msgFactoryName = msgFactoryName.trim();
        Preconditions.checkArgument(StringUtils.isNotBlank(msgFactoryName),
                "msgFactoryName is empty");

        serviceDecoderName =
                context.getString(ConfigConstants.SERVICE_PROCESSOR_NAME,
                        "org.apache.inlong.dataproxy.source.DefaultServiceDecoder");
        serviceDecoderName = serviceDecoderName.trim();
        Preconditions.checkArgument(StringUtils.isNotBlank(serviceDecoderName),
                "serviceProcessorName is empty");

        messageHandlerName =
                context.getString(ConfigConstants.MESSAGE_HANDLER_NAME,
                        "org.apache.inlong.dataproxy.source.ServerMessageHandler");
        messageHandlerName = messageHandlerName.trim();
        Preconditions.checkArgument(StringUtils.isNotBlank(messageHandlerName),
                "messageHandlerName is empty");

        maxMsgLength = context.getInteger(ConfigConstants.MAX_MSG_LENGTH, 1024 * 64);
        Preconditions.checkArgument(
                (maxMsgLength >= 4 && maxMsgLength <= ConfigConstants.MSG_MAX_LENGTH_BYTES),
                "maxMsgLength must be >= 4 and <= " + ConfigConstants.MSG_MAX_LENGTH_BYTES);
        isCompressed = context.getBoolean(ConfigConstants.MSG_COMPRESSED, true);
    }

    /**
     * get metricItemSet
     * @return the metricItemSet
     */
    public DataProxyMetricItemSet getMetricItemSet() {
        return metricItemSet;
    }
}
