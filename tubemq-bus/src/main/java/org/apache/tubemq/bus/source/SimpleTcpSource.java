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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.tubemq.bus.monitor.CounterGroup;
import org.apache.tubemq.bus.monitor.CounterGroupExt;
import org.apache.tubemq.bus.monitor.MonitorIndex;
import org.apache.tubemq.bus.monitor.MonitorIndexExt;
import org.apache.tubemq.bus.util.HandlerConfigFromFile;
import org.apache.tubemq.bus.util.NamedThreadFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTcpSource extends BaseSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(SimpleTcpSource.class);
    private static final String MIN_FAIL_METRIC_NAME = "l5_tdbus_fail_min";
    private static final String BLACKLIST_FILE_PATH = "blacklist.properties";
    public static ArrayList<String> blacklist = new ArrayList<String>();
    private static long propsLastModified;
    private final AtomicInteger workerIdCounter = new AtomicInteger(0);
    private final HandlerConfigFromFile handlerConfig = new HandlerConfigFromFile();
    private int coreSize = 30;
    private int capactiy = 100000;
    private int bossCount = 1;
    private int workerCount = 10;
    private int writeBufferHighWaterMark = 5 * 1024 * 1024;
    private Channel nettyChannel = null;
    private ServerBootstrap serverBootstrap = null;
    private boolean tcpNoDelay = true;
    private boolean keepAlive = true;
    private int receiveBufferSize;
    private int highWaterMark;

    //    private Counter minCounter,minFailCounter;
//    private  TDConfigSDKNGClient client=null;
    private int sendBufferSize;
    private int trafficClass;
    private int maxThreads = 32;
    private String metricTopicPrefix = "teg_tdbank_tmertic";
    private CheckBlackListThread checkBlackListThread;
    private boolean isOldMetircOn = true;
    private boolean isNewMetricOn = true;
    private MonitorIndex monitorIndex;
    private MonitorIndex apiVersion;
    private int maxMonitorCnt = 300000;
    private String set;
    private String cluserId;

    public SimpleTcpSource() {
        super();
    }

    public static void checkBlackList(ArrayList blacklist, ChannelGroup allChannels) {//断开已建立好的连接
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
                        logger.warn(
                                "fail to get the remote IP, and strIP={},remoteSocketAddress={}",
                                strRemoteIP,
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

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void start() {
        logger.info("start " + this.getName());
        checkBlackListThread = new CheckBlackListThread();
        checkBlackListThread.start();
        super.start();
        if (isNewMetricOn) {
            monitorIndex = new MonitorIndex("Source", 60, maxMonitorCnt);
            apiVersion = new MonitorIndex("ApiVersion", 60, maxMonitorCnt);
        }
        MonitorIndexExt monitorIndexExt = new MonitorIndexExt("TDBus_monitors#tcp", 60,
                maxMonitorCnt);

        //此处可让netty线程不被重命名，让threadfactory发挥作用
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
        ChannelFactory factory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(
                                new NamedThreadFactory("tcpSource-nettyBoss-threadGroup")), 1,
                        Executors.newCachedThreadPool(
                                new NamedThreadFactory("tcpSource-nettyWorker-threadGroup")),
                        maxThreads);
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

            // channelprocessor, countergroup, type, maxlength, topic, attr,
            // filteremptymsg
            Constructor ctor =
                    clazz.getConstructor(ChannelProcessor.class, ChannelGroup.class,
                            CounterGroup.class,
                            CounterGroupExt.class, String.class, ServiceDecoder.class, String.class,
                            Integer.class, String.class, String.class, Boolean.class, Integer.class,
                            Boolean.class,
                            Boolean.class, MonitorIndex.class, MonitorIndex.class,
                            MonitorIndexExt.class, String.class,
                            HandlerConfigFromFile.class);

            logger.info("Using channel processor:{}", getChannelProcessor().getClass().getName());
            fac = (ChannelPipelineFactory) ctor.newInstance(getChannelProcessor(), allChannels,
                    counterGroup, counterGroupExt, "tcp", serviceDecoder, messageHandlerName,
                    maxMsgLength, topic, attr, filterEmptyMsg, maxConnections, isCompressed,
                    isNewMetricOn, monitorIndex, apiVersion, monitorIndexExt, this.getName(),
                    handlerConfig);


        } catch (Exception e) {
            logger.error(
                    "Simple Tcp Source start error, fail to construct ChannelPipelineFactory with name {}, ex {}",
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
            logger.error("Simple TCP Source error bind host {} port {},program will exit!",
                    new Object[]{ host, port });
            System.exit(-1);
        }

        allChannels.add(nettyChannel);
        logger.info("Simple TCP Source started at host {}, port {}", host, port);
    }

    @Override
    public void stop() {
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
        logger.info(context.toString());

        super.configure(context);

        tcpNoDelay = context.getBoolean(ConfigConstants.TCP_NO_DELAY, true);

        keepAlive = context.getBoolean(ConfigConstants.KEEP_ALIVE, true);
        highWaterMark = context.getInteger(ConfigConstants.HIGH_WATER_MARK, 64 * 1024);
        receiveBufferSize = context.getInteger(ConfigConstants.RECEIVE_BUFFER_SIZE, 1024 * 64);
        if (receiveBufferSize > 16 * 1024 * 1024) {
            receiveBufferSize = 16 * 1024 * 1024;
        }
        checkArgument(receiveBufferSize > 0, "receiveBufferSize must be > 0");

        sendBufferSize = context.getInteger(ConfigConstants.SEND_BUFFER_SIZE, 1024 * 64);
        if (sendBufferSize > 16 * 1024 * 1024) {
            sendBufferSize = 16 * 1024 * 1024;
        }
        checkArgument(sendBufferSize > 0, "sendBufferSize must be > 0");

        trafficClass = context.getInteger(ConfigConstants.TRAFFIC_CLASS, 0);
        checkArgument((trafficClass == 0 || trafficClass == 96),
                "trafficClass must be == 0 or == 96");

        try {
            maxThreads = context.getInteger(ConfigConstants.MAX_THREADS, 32);
        } catch (NumberFormatException e) {
            logger.warn("Simple TCP Source max-threads property must specify an integer value. {}",
                    context.getString(ConfigConstants.MAX_THREADS));
        }

        // 指标统计初始化.
        isOldMetircOn = context.getBoolean("old-metric-on", true);
        isNewMetricOn = context.getBoolean("new-metric-on", true);
        maxMonitorCnt = context.getInteger("max-monitor-cnt", 300000);

        coreSize = context.getInteger("l5-coresize", 30);
        capactiy = context.getInteger("l5-capacity", 100000);
        bossCount = context.getInteger("l5-bosscount", 1);
        workerCount = context.getInteger("l5-workcount", 10);
        writeBufferHighWaterMark = context
                .getInteger("l5-writeBufferHighWaterMark", 5 * 1024 * 1024);

        metricTopicPrefix = context.getString("metric_topic_prefix", "teg_tdbank_tmertic");


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
                    File blacklistFile = new File("conf/" + BLACKLIST_FILE_PATH);
                    if (blacklistFile.lastModified() > propsLastModified) {
                        ArrayList<String> tmpblacklist = load(BLACKLIST_FILE_PATH);
                        blacklist = tmpblacklist;
                        propsLastModified = blacklistFile.lastModified();
                        SimpleDateFormat formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        logger.info("blacklist.properties:{}\n{}",
                                formator.format(new Date(blacklistFile.lastModified())), blacklist);
                    }
//                    Channel[] channels=(Channel[])allChannels.toArray();
                    checkBlackList(blacklist, allChannels);
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    logger.info("ConfigReloader thread exit!");
                    return;
                } catch (Throwable t) {
                    logger.error("ConfigReloader thread error!", t);
                }
            }
        }
    }
}
