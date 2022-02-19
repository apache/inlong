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

package org.apache.inlong.dataproxy.sink;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;

import java.util.HashMap;
import org.apache.inlong.commons.metrics.metric.MetricRegister;
import org.apache.inlong.dataproxy.base.HighPriorityThreadFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.inlong.commons.monitor.LogCounter;
import org.apache.inlong.commons.monitor.MonitorIndex;
import org.apache.inlong.commons.monitor.MonitorIndexExt;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.holder.ConfigUpdateCallback;
import org.apache.inlong.dataproxy.config.pojo.PulsarConfig;
import org.apache.inlong.dataproxy.consts.AttributeConstants;
import org.apache.inlong.dataproxy.consts.ConfigConstants;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItem;
import org.apache.inlong.dataproxy.metrics.DataProxyMetricItemSet;
import org.apache.inlong.dataproxy.sink.pulsar.CreatePulsarClientCallBack;
import org.apache.inlong.dataproxy.sink.pulsar.PulsarClientService;
import org.apache.inlong.dataproxy.sink.pulsar.SendMessageCallBack;
import org.apache.inlong.dataproxy.utils.FailoverChannelProcessorHolder;
import org.apache.inlong.dataproxy.utils.NetworkUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError;
import org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * use pulsarSink need adding such config, if these ara not config in flume.conf, PulsarSink will
 * use default value.
 * prefix of pulsar sink config in flume.conf like this XXX.sinks.XXX.property
 * and properties are may these Configurations:
 *  type (*): value must be 'org.apache.inlong.dataproxy.sink.PulsarSink'
 *  pulsar_server_url_list (*): value is pulsar broker url , like this 'pulsar://127.0.0.1:6650'
 *  send_timeout_MILL: send message timeout, unit is millisecond, default value is 30000 (mean 30s)
 *  stat_interval_sec: stat info will be made period time , unit is second, default value is 60s
 *  thread-num: sink thread num. default value  is 8
 *  client-id-cache: whether use cache in client, default value is true
 *  max_pending_messages: default value is 10000
 *  max_batching_messages: default value is 1000
 *  enable_batch: default is true
 *  block_if_queue_full: default is true
 */
public class PulsarSink extends AbstractSink implements Configurable,
        SendMessageCallBack, CreatePulsarClientCallBack {

    private static final Logger logger = LoggerFactory.getLogger(PulsarSink.class);

    /*
     * properties for header info
     */
    private static String TOPIC = "topic";

    /*
     * default value
     */
    private static int BATCH_SIZE = 10000;

    /*
     * for log
     */
    private RateLimiter diskRateLimiter;

    /*
     * for stat
     */
    private AtomicLong currentSuccessSendCnt = new AtomicLong(0);
    private AtomicLong lastSuccessSendCnt = new AtomicLong(0);
    private long t1 = System.currentTimeMillis();
    private long t2 = 0L;
    private static AtomicLong totalPulsarSuccSendCnt = new AtomicLong(0);
    private static AtomicLong totalPulsarSuccSendSize = new AtomicLong(0);
    /*
     * for control
     */
    private boolean overflow = false;

    private LinkedBlockingQueue<EventStat> resendQueue;

    private int maxMonitorCnt = 300000;

    private long logCounter = 0;

    private final AtomicLong currentInFlightCount = new AtomicLong(0);

    private static ConcurrentHashMap<String, Long> illegalTopicMap =
            new ConcurrentHashMap<String, Long>();

    /*
     * whether the SendTask thread can send data to pulsar
     */
    private volatile boolean canSend = false;

    /*
     * Control whether the SinkRunner thread can read data from the Channel
     */
    private volatile boolean canTake = false;

    /*
     * log tools
     */
    private static final LogCounter logPrinterA = new LogCounter(10, 100000, 60 * 1000);
    private static final LogCounter logPrinterB = new LogCounter(10, 100000, 60 * 1000);
    private static final LogCounter logPrinterC = new LogCounter(10, 100000, 60 * 1000);

    private static final String SINK_THREAD_NUM = "thread-num";
    private int eventQueueSize = 10000;
    private int badEventQueueSize = 10000;
    private int threadNum;

    /*
     * send thread pool
     */
    private Thread[] sinkThreadPool;
    private PulsarClientService pulsarClientService;
    private LinkedBlockingQueue<Event> eventQueue;

    private static final String SEPARATOR = "#";
    private boolean isNewMetricOn = true;


    private MonitorIndex monitorIndex;
    private MonitorIndexExt monitorIndexExt;
    private SinkCounter sinkCounter;

    /*
     *  metric
     */
    private Map<String, String> dimensions;
    private DataProxyMetricItemSet metricItemSet;

    private ConfigManager configManager;
    private Map<String, String> topicProperties;

    private Map<String, String> pulsarCluster;
    private PulsarConfig pulsarConfig;

    private static final Long PRINT_INTERVAL = 30L;

    private static final PulsarPerformanceTask pulsarPerformanceTask = new PulsarPerformanceTask();

    private static ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(1, new HighPriorityThreadFactory("pulsarPerformance-Printer-thread"));

    private static final  LoadingCache<String, Long> agentIdCache = CacheBuilder.newBuilder()
            .concurrencyLevel(4 * 8).initialCapacity(500).expireAfterAccess(30, TimeUnit.SECONDS)
            .build(new CacheLoader<String, Long>() {
                @Override
                public Long load(String key) {
                    return System.currentTimeMillis();
                }
            });

    static {
        /*
         * stat pulsar performance
         */
        logger.info("pulsarPerformanceTask!!!!!!");
        scheduledExecutorService.scheduleWithFixedDelay(pulsarPerformanceTask, 0L,
                PRINT_INTERVAL, TimeUnit.SECONDS);
    }

    public PulsarSink() {
        super();
        logger.debug("new instance of PulsarSink!");
    }

    /**
     * configure
     * @param context
     */
    @Override
    public void configure(Context context) {
        logger.info("PulsarSink started and context = {}", context.toString());
        isNewMetricOn = context.getBoolean("new-metric-on", true);
        maxMonitorCnt = context.getInteger("max-monitor-cnt", 300000);

        configManager = ConfigManager.getInstance();
        topicProperties = configManager.getTopicProperties();
        pulsarCluster = configManager.getPulsarUrl2Token();
        pulsarConfig = configManager.getPulsarConfig(); //pulsar common config
        pulsarClientService = new PulsarClientService(pulsarConfig);
        configManager.getTopicConfig().addUpdateCallback(new ConfigUpdateCallback() {
            @Override
            public void update() {
                if (pulsarClientService != null) {
                    diffSetPublish(pulsarClientService,
                            new HashSet<String>(topicProperties.values()),
                            new HashSet<String>(configManager.getTopicProperties().values()));
                }
            }
        });
        configManager.getPulsarCluster().addUpdateCallback(new ConfigUpdateCallback() {
            @Override
            public void update() {
                diffRestartPulsarClient(pulsarCluster.keySet(), configManager.getPulsarUrl2Token().keySet());
            }
        });
        badEventQueueSize = pulsarConfig.getBadEventQueueSize();
        resendQueue = new LinkedBlockingQueue<EventStat>(badEventQueueSize);

        Preconditions.checkArgument(pulsarConfig.getThreadNum() > 0, "threadNum must be > 0");
        sinkThreadPool = new Thread[pulsarConfig.getThreadNum()];
        eventQueueSize = pulsarConfig.getEventQueueSize();
        eventQueue = new LinkedBlockingQueue<Event>(eventQueueSize);

        if (pulsarConfig.getDiskIoRatePerSec() != 0) {
            diskRateLimiter = RateLimiter.create(pulsarConfig.getDiskIoRatePerSec());
        }

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    private void initTopicSet(PulsarClientService pulsarClientService, Set<String> topicSet) {
        long startTime = System.currentTimeMillis();
        if (topicSet != null) {
            for (String topic : topicSet) {
                pulsarClientService.initTopicProducer(topic);
            }
        }
        logger.info(getName() + " initTopicSet cost: "
                + (System.currentTimeMillis() - startTime) + "ms");
        logger.info(getName() + " producer is ready for topics : "
                + pulsarClientService.getProducerInfoMap().keySet());
    }

    /**
     * When topic.properties is re-enabled, the producer update is triggered
     * @param originalSet
     * @param endSet
     */
    public void diffSetPublish(PulsarClientService pulsarClientService, Set<String> originalSet,
            Set<String> endSet) {
        boolean changed = false;
        for (String s : endSet) {
            if (!originalSet.contains(s)) {
                changed = true;
                try {
                    pulsarClientService.initTopicProducer(s);
                } catch (Exception e) {
                    logger.error("Get producer failed!", e);
                }
            }
        }
        if (changed) {
            logger.info("topics.properties has changed, trigger diff publish for {}", getName());
            topicProperties = configManager.getTopicProperties();
        }
    }

    /**
     * When pulsarURLList change, close and restart
     *
     * @param originalCluster
     * @param endCluster
     */
    public void diffRestartPulsarClient(Set<String> originalCluster, Set<String> endCluster) {
        if (!originalCluster.equals(endCluster)) {
            logger.info("pulsarConfig has changed, close current pulsarClientService and restart");
            pulsarClientService.close();

            pulsarCluster = configManager.getPulsarUrl2Token();
            configManager.getPulsarConfig().setUrl2token(pulsarCluster);
            pulsarClientService.initCreateConnection(this);
            try {
                initTopicSet(pulsarClientService, new HashSet<String>(topicProperties.values()));
            } catch (Exception e) {
                logger.info("pulsar sink restart, publish topic fail.", e);
            }

        }
    }

    @Override
    public void start() {
        logger.info("pulsar sink starting...");
        this.dimensions = new HashMap<>();
        this.dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, "DataProxy");
        this.dimensions.put(DataProxyMetricItem.KEY_SINK_ID, this.getName());
        //register metrics
        this.metricItemSet = new DataProxyMetricItemSet(this.getName());
        MetricRegister.register(metricItemSet);

        sinkCounter.start();
        pulsarClientService.initCreateConnection(this);
        int statIntervalSec = pulsarConfig.getStatIntervalSec();
        Preconditions.checkArgument(statIntervalSec >= 0, "statIntervalSec must be >= 0");
        if (statIntervalSec > 0) {
            /*
             * switch for lots of metrics
             */
            if (isNewMetricOn) {
                monitorIndex = new MonitorIndex("Pulsar_Sink", statIntervalSec, maxMonitorCnt);
            }
            monitorIndexExt = new MonitorIndexExt("Pulsar_Sink_monitors#" + this.getName(),
                    statIntervalSec, maxMonitorCnt);
        }

        super.start();
        this.canSend = true;
        this.canTake = true;

        for (int i = 0; i < sinkThreadPool.length; i++) {
            try {
                initTopicSet(pulsarClientService,
                        new HashSet<String>(topicProperties.values()));
            } catch (Exception e) {
                logger.info("pulsar sink start publish topic fail.", e);
            }
            sinkThreadPool[i] = new Thread(new SinkTask(pulsarClientService), getName()
                    + "_pulsar_sink_sender-"
                    + i);
            sinkThreadPool[i].start();
        }
        logger.debug("pulsar sink started");
    }

    @Override
    public void stop() {
        logger.info("pulsar sink stopping");
        this.canTake = false;
        int waitCount = 0;
        while (eventQueue.size() != 0 && waitCount++ < 10) {
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                logger.info("Stop thread has been interrupt!");
                break;
            }
        }
        this.canSend = false;
        if (pulsarConfig.getStatIntervalSec() > 0) {
            try {
                monitorIndex.shutDown();
            } catch (Exception e) {
                logger.warn("stat runner interrupted");
            }
        }
        if (pulsarClientService != null) {
            pulsarClientService.close();
        }
        if (sinkThreadPool != null) {
            for (Thread thread : sinkThreadPool) {
                if (thread != null) {
                    thread.interrupt();
                }
            }
            sinkThreadPool = null;
        }

        super.stop();
        if (!scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
        sinkCounter.stop();
        logger.debug("pulsar sink stopped. Metrics:{}", sinkCounter);
    }

    @Override
    public Status process() throws EventDeliveryException {
        if (!this.canTake) {
            return Status.BACKOFF;
        }

        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        try {
            Event event = channel.take();
            if (event != null) {
                if (diskRateLimiter != null) {
                    diskRateLimiter.acquire(event.getBody().length);
                }
                if (!eventQueue.offer(event, 3 * 1000, TimeUnit.MILLISECONDS)) {
                    logger.info("[{}] Channel --> Queue(has no enough space,current code point) "
                            + "--> pulsar,Check if pulsar server or network is ok.(if this situation "
                            + "last long time it will cause memoryChannel full and fileChannel write.)", getName());
                    tx.rollback();
                    // metric
                    if (event.getHeaders().containsKey(TOPIC)) {
                        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, event.getHeaders().get(TOPIC));
                    } else {
                        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, "");
                    }
                    DataProxyMetricItem metricItem = this.metricItemSet.findMetricItem(dimensions);
                    metricItem.readFailCount.incrementAndGet();
                    metricItem.readFailSize.addAndGet(event.getBody().length);
                } else {
                    tx.commit();
                    // metric
                    if (event.getHeaders().containsKey(TOPIC)) {
                        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, event.getHeaders().get(TOPIC));
                    } else {
                        dimensions.put(DataProxyMetricItem.KEY_SINK_DATA_ID, "");
                    }
                    DataProxyMetricItem metricItem = this.metricItemSet.findMetricItem(dimensions);
                    metricItem.readSuccessCount.incrementAndGet();
                    metricItem.readFailSize.addAndGet(event.getBody().length);
                }
            } else {
                status = Status.BACKOFF;
                tx.commit();
            }
        } catch (Throwable t) {
            logger.error("Process event failed!" + this.getName(), t);
            try {
                tx.rollback();
            } catch (Throwable e) {
                logger.error("pulsar sink transaction rollback exception", e);

            }
        } finally {
            tx.close();
        }
        return status;
    }

    private void editStatistic(final Event event, String keyPostfix, String msgId) {
        String topic = "";
        String streamId = "";
        String nodeIp = null;
        if (event != null) {
            if (event.getHeaders().containsKey(TOPIC)) {
                topic = event.getHeaders().get(TOPIC);
            }
            if (event.getHeaders().containsKey(AttributeConstants.INTERFACE_ID)) {
                streamId = event.getHeaders().get(AttributeConstants.INTERFACE_ID);
            } else if (event.getHeaders().containsKey(AttributeConstants.INAME)) {
                streamId = event.getHeaders().get(AttributeConstants.INAME);
            }

            /*
             * Compatible agent
             */
            if (event.getHeaders().containsKey("ip")) {
                event.getHeaders().put(ConfigConstants.REMOTE_IP_KEY, event.getHeaders().get("ip"));
                event.getHeaders().remove("ip");
            }

            /*
             * Compatible agent
             */
            if (event.getHeaders().containsKey("time")) {
                event.getHeaders().put(AttributeConstants.DATA_TIME, event.getHeaders().get("time"));
                event.getHeaders().remove("time");
            }

            if (event.getHeaders().containsKey(ConfigConstants.REMOTE_IP_KEY)) {
                nodeIp = event.getHeaders().get(ConfigConstants.REMOTE_IP_KEY);
                if (event.getHeaders().containsKey(ConfigConstants.REMOTE_IDC_KEY)) {

                    if (nodeIp != null) {
                        nodeIp = nodeIp.split(":")[0];
                    }

                    long tMsgCounterL = 1L;
                    /*
                     * msg counter
                     */
                    if (event.getHeaders().containsKey(ConfigConstants.MSG_COUNTER_KEY)) {
                        tMsgCounterL = Integer.parseInt(event.getHeaders()
                                .get(ConfigConstants.MSG_COUNTER_KEY));
                    }

                    /*
                     * SINK_INTF#pulsarsink1#topic#streamId#clientIp#busIP#pkgTime#successCnt
                     * #packcnt
                     * #packsize#failCnt
                     */
                    StringBuilder newbase = new StringBuilder();
                    newbase.append(this.getName()).append(SEPARATOR).append(topic).append(SEPARATOR)
                            .append(streamId).append(SEPARATOR).append(nodeIp)
                            .append(SEPARATOR).append(NetworkUtils.getLocalIp())
                            .append(SEPARATOR).append(SEPARATOR)
                            .append(event.getHeaders().get(ConfigConstants.PKG_TIME_KEY));

                    long messageSize = event.getBody().length;

                    if (event.getHeaders().get(ConfigConstants.TOTAL_LEN) != null) {
                        messageSize = Long.parseLong(event.getHeaders().get(ConfigConstants.TOTAL_LEN));
                    }

                    if (keyPostfix != null && !keyPostfix.equals("")) {
                        monitorIndex.addAndGet(new String(newbase), 0, 0, 0, (int) tMsgCounterL);
                        if (logPrinterB.shouldPrint()) {
                            logger.warn("error cannot send event, {} event size is {}", topic, messageSize);
                        }
                    } else {
                        monitorIndex.addAndGet(new String(newbase), (int) tMsgCounterL, 1, messageSize, 0);
                    }
                }
            }
        }
    }

    @Override
    public void handleCreateClientSuccess(String url) {
        logger.info("createConnection success for url = {}", url);
        sinkCounter.incrementConnectionCreatedCount();
    }

    @Override
    public void handleCreateClientException(String url) {
        logger.info("createConnection has exception for url = {}", url);
        sinkCounter.incrementConnectionFailedCount();
    }

    @Override
    public void handleMessageSendSuccess(String topic, Object result,  EventStat eventStat) {
        /*
         * Statistics pulsar performance
         */
        totalPulsarSuccSendCnt.incrementAndGet();
        totalPulsarSuccSendSize.addAndGet(eventStat.getEvent().getBody().length);
        /*
         *add to sinkCounter
         */
        sinkCounter.incrementEventDrainSuccessCount();
        currentInFlightCount.decrementAndGet();
        currentSuccessSendCnt.incrementAndGet();
        long nowCnt = currentSuccessSendCnt.get();
        long oldCnt = lastSuccessSendCnt.get();
        long logEveryNEvents = pulsarConfig.getLogEveryNEvents();
        Preconditions.checkArgument(logEveryNEvents > 0, "logEveryNEvents must be > 0");

        if (nowCnt % logEveryNEvents == 0 && nowCnt != lastSuccessSendCnt.get()) {
            lastSuccessSendCnt.set(nowCnt);
            t2 = System.currentTimeMillis();
            logger.info("Pulsar sink {}, succ put {} events to pulsar,"
                    + " in the past {} millsec", new Object[] {
                    getName(), (nowCnt - oldCnt), (t2 - t1)
            });
            t1 = t2;
        }
        Map<String, String> dimensions =  getNewDimension(DataProxyMetricItem.KEY_SINK_DATA_ID,
                topic);
        DataProxyMetricItem metricItem = this.metricItemSet.findMetricItem(dimensions);
        metricItem.sendSuccessCount.incrementAndGet();
        metricItem.sendSuccessSize.addAndGet(eventStat.getEvent().getBody().length);
        metricItem.sendCount.incrementAndGet();
        metricItem.sendSize.addAndGet(eventStat.getEvent().getBody().length);
        monitorIndexExt.incrementAndGet("PULSAR_SINK_SUCCESS");
        editStatistic(eventStat.getEvent(), null, result.toString());

    }

    @Override
    public void handleMessageSendException(String topic, EventStat eventStat,  Object e) {
        monitorIndexExt.incrementAndGet("PULSAR_SINK_EXP");
        if (e instanceof TooLongFrameException) {
            PulsarSink.this.overflow = true;
        } else if (e instanceof ProducerQueueIsFullError) {
            PulsarSink.this.overflow = true;
        } else if (!(e instanceof AlreadyClosedException
                || e instanceof PulsarClientException.NotConnectedException
                || e instanceof TopicTerminatedException)) {
            if (logPrinterB.shouldPrint()) {
                logger.error("Send failed!{}{}", getName(), e);
            }
            if (eventStat.getRetryCnt() == 0) {
                editStatistic(eventStat.getEvent(), "failure", "");
            }
        }
        eventStat.incRetryCnt();
        Map<String, String> dimensions =  getNewDimension(DataProxyMetricItem.KEY_SINK_DATA_ID,
                topic);
        DataProxyMetricItem metricItem = this.metricItemSet.findMetricItem(dimensions);
        metricItem.sendFailCount.incrementAndGet();
        metricItem.sendFailSize.addAndGet(eventStat.getEvent().getBody().length);
        resendEvent(eventStat, true);
    }

    /**
     * Resend the data and store the data in the memory cache.
     * @param es
     * @param isDecrement
     */
    private void resendEvent(EventStat es, boolean isDecrement) {
        try {
            if (isDecrement) {
                currentInFlightCount.decrementAndGet();
            }
            if (es == null || es.getEvent() == null) {
                return;
            }
            /*
             * If the failure requires retransmission to pulsar,
             * the sid needs to be removed before retransmission.
             */
            if (pulsarConfig.getClientIdCache()) {
                String clientId = es.getEvent().getHeaders().get(ConfigConstants.SEQUENCE_ID);
                if (clientId != null && agentIdCache.asMap().containsKey(clientId)) {
                    agentIdCache.invalidate(clientId);
                }
            }
            if (!resendQueue.offer(es)) {
                /*
                 * If the retry queue is full, it will re-enter the channel; if it is not full,
                 *  it will be taken out of the resendQueue in sinktask
                 */
                FailoverChannelProcessorHolder.getChannelProcessor().processEvent(es.getEvent());
                if (logPrinterC.shouldPrint()) {
                    logger.error(getName() + " Channel --> pulsar --> ResendQueue(full) "
                            + "-->FailOverChannelProcessor(current code point), "
                            + "Resend queue is full,Check if pulsar server or network is ok.");
                }
            }
        } catch (Throwable throwable) {
            monitorIndexExt.incrementAndGet("PULSAR_SINK_DROPPED");
            if (logPrinterC.shouldPrint()) {
                logger.error(getName() + " Discard msg because put events to both of "
                        + "queue and fileChannel fail,current resendQueue.size = "
                        + resendQueue.size(), throwable);
            }
        }
    }

    private Map getNewDimension(String otherKey, String value) {
        Map dimensions = new HashMap<>();
        dimensions.put(DataProxyMetricItem.KEY_CLUSTER_ID, "DataProxy");
        dimensions.put(DataProxyMetricItem.KEY_SINK_ID, this.getName());
        dimensions.put(otherKey, value);
        return dimensions;
    }

    static class PulsarPerformanceTask implements Runnable {
        @Override
        public void run() {
            try {
                if (totalPulsarSuccSendSize.get() != 0) {
                    logger.info("Total pulsar performance tps :"
                            + totalPulsarSuccSendCnt.get() / PRINT_INTERVAL
                            + "/s, avg msg size:"
                            + totalPulsarSuccSendSize.get() / totalPulsarSuccSendCnt.get()
                            + ",print every " + PRINT_INTERVAL + " seconds");
                    /*
                     * totalpulsarSuccSendCnt represents the number of packets
                     */
                    totalPulsarSuccSendCnt.set(0);
                    totalPulsarSuccSendSize.set(0);
                }

            } catch (Exception e) {
                logger.info("pulsarPerformanceTask error", e);
            }
        }
    }

    class SinkTask implements Runnable {

        private PulsarClientService pulsarClientService;

        public SinkTask(PulsarClientService pulsarClientService) {
            this.pulsarClientService = pulsarClientService;
        }

        @Override
        public void run() {
            logger.info("Sink task {} started.", Thread.currentThread().getName());
            while (canSend) {
                boolean decrementFlag = false;
                Event event = null;
                EventStat eventStat = null;
                String topic = null;
                try {
                    if (PulsarSink.this.overflow) {
                        PulsarSink.this.overflow = false;
                        Thread.currentThread().sleep(10);
                    }
                    if (!resendQueue.isEmpty()) {
                        /*
                         * Send the data in the retry queue first
                         */
                        eventStat = resendQueue.poll();
                        if (eventStat != null) {
                            event = eventStat.getEvent();
                            // logger.warn("Resend event: {}", event.toString());
                            if (event.getHeaders().containsKey(TOPIC)) {
                                topic = event.getHeaders().get(TOPIC);
                            }
                        }
                    } else {
                        if (currentInFlightCount.get() > BATCH_SIZE) {
                            /*
                             * Under the condition that the number of unresponsive messages
                             * is greater than 1w, the number of unresponsive messages sent
                             * to pulsar will be printed periodically
                             */
                            logCounter++;
                            if (logCounter == 1 || logCounter % 100000 == 0) {
                                logger.info(getName()
                                                + " currentInFlightCount={} resendQueue"
                                                + ".size={}",
                                        currentInFlightCount.get(), resendQueue.size());
                            }
                            if (logCounter > Long.MAX_VALUE - 10) {
                                logCounter = 0;
                            }
                        }
                        event = eventQueue.take();
                        eventStat = new EventStat(event);
                        sinkCounter.incrementEventDrainAttemptCount();
                        if (event.getHeaders().containsKey(TOPIC)) {
                            topic = event.getHeaders().get(TOPIC);
                        }
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Event is {}, topic = {} ", event, topic);
                    }

                    if (event == null) {
                        continue;
                    }

                    if (topic == null || topic.equals("")) {
                        logger.warn("no topic specified in event header, just skip this event");
                        continue;
                    }

                    Long expireTime = illegalTopicMap.get(topic);
                    if (expireTime != null) {
                        long currentTime = System.currentTimeMillis();
                        if (expireTime > currentTime) {
                            /*
                             * If the exception-channel is configured, put the exception-channel,
                             * otherwise discard the data
                             */
                            continue;
                        } else {
                            /*
                             * Illegal has expired, there is no need to put it in the map
                             */
                            illegalTopicMap.remove(topic);
                        }
                    }

                    String clientId = event.getHeaders().get(ConfigConstants.SEQUENCE_ID);
                    final EventStat es = eventStat;

                    boolean hasKey = false;
                    if (pulsarConfig.getClientIdCache() && clientId != null) {
                        hasKey = agentIdCache.asMap().containsKey(clientId);
                    }

                    if (pulsarConfig.getClientIdCache() && clientId != null && hasKey) {
                        agentIdCache.put(clientId, System.currentTimeMillis());
                        if (logPrinterA.shouldPrint()) {
                            logger.info("{} agent package {} existed,just discard.",
                                    getName(), clientId);
                        }
                    } else {
                        if (pulsarConfig.getClientIdCache() && clientId != null) {
                            agentIdCache.put(clientId, System.currentTimeMillis());
                        }
                        boolean sendResult = pulsarClientService.sendMessage(topic, event,
                                PulsarSink.this, es);
                        /*
                         * handle producer is current is null
                         */
                        if (!sendResult) {
                            illegalTopicMap.put(topic, System.currentTimeMillis() + 30 * 1000);
                            continue;
                        }
                        currentInFlightCount.incrementAndGet();
                        decrementFlag = true;
                    }
                    /*
                     * No exception is thrown, after a complete one-time sending,
                     * the topic can be deleted from the illegal list
                     */
                    illegalTopicMap.remove(topic);
                } catch (InterruptedException e) {
                    logger.info("Thread {} has been interrupted!", Thread.currentThread().getName());
                    return;
                } catch (Throwable t) {
                    if (t instanceof PulsarClientException) {
                        String message = t.getMessage();
                        if (message != null && (message.contains("No available queue for topic")
                                || message.contains("The brokers of topic are all forbidden"))) {
                            illegalTopicMap.put(topic, System.currentTimeMillis() + 60 * 1000);
                            logger.info("IllegalTopicMap.put " + topic);
                            continue;
                        } else {
                            try {
                                /*
                                 * The exception of pulsar will cause the sending thread to block
                                 * and prevent further pressure on pulsar. Here you should pay
                                 * attention to the type of exception to prevent the error of
                                 *  a topic from affecting the global
                                 */
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                //ignore..
                            }
                        }
                    }
                    if (logPrinterA.shouldPrint()) {
                        logger.error("Sink task fail to send the message, decrementFlag="
                                + decrementFlag
                                + ",sink.name="
                                + Thread.currentThread().getName()
                                + ",event.headers="
                                + eventStat.getEvent().getHeaders(), t);
                    }
                    /*
                     * producer.sendMessage is abnormal,
                     * so currentInFlightCount is not added,
                     * so there is no need to subtract
                     */
                    resendEvent(eventStat, decrementFlag);
                }
            }
        }
    }
}
