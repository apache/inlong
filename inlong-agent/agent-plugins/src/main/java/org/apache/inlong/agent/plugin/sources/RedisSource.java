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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.impl.DefaultCommand;
import com.moilioncircle.redis.replicator.cmd.parser.DefaultCommandParser;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueHash;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueList;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueSet;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueZSet;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis source
 */
public class RedisSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSource.class);
    private static final long MAX_DATA_SIZE = 500 * 1024;
    private static final int REDIS_QUEUE_SIZE = 10000;
    private static final long DEFAULT_FREQ = 60 * 1000;
    private static final String GET_COMMAND = "GET";
    private static final String MGET_COMMAND = "MGET";
    private static final String HGET_COMMAND = "HGET";
    private static final String ZSCORE_COMMAND = "ZSCORE";
    private static final String ZREVRANK_COMMAND = "ZREVRANK";
    private static final String EXISTS_COMMAND = "EXISTS";
    private Gson gson;

    public InstanceProfile profile;
    private String port;
    private Jedis jedis;
    private String hostName;
    private boolean ssl;
    private String authUser;
    private String authPassword;
    private String readTimeout;
    private String replId;
    private String snapShot;
    private String dbName;
    private String redisCommand;

    private String fieldOrMember;
    private boolean destroyed;
    private boolean isSubscribe;
    private Set<String> keys;
    private Set<String> subscribeOperations;
    private Replicator redisReplicator;
    private BlockingQueue<SourceData> redisQueue;
    private ScheduledExecutorService executor;

    // Command handler map
    private static final Map<String, CommandHandler> commandHandlers = Maps.newConcurrentMap();

    static {
        commandHandlers.put(GET_COMMAND, RedisSource::handleGet);
        commandHandlers.put(MGET_COMMAND, RedisSource::handleMGet);
        commandHandlers.put(HGET_COMMAND, RedisSource::handleHGet);
        commandHandlers.put(ZSCORE_COMMAND, RedisSource::handleZScore);
        commandHandlers.put(ZREVRANK_COMMAND, RedisSource::handleZRevRank);
        commandHandlers.put(EXISTS_COMMAND, RedisSource::handleExists);
    }

    public RedisSource() {

    }

    @Override
    protected String getThreadName() {
        return "redis-source-" + taskId + "-" + instanceId;
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        LOGGER.info("Redis Source init: {}", profile.toJsonStr());
        this.port = profile.get(TaskConstants.TASK_REDIS_PORT);
        this.hostName = profile.get(TaskConstants.TASK_REDIS_HOSTNAME);
        this.ssl = profile.getBoolean(TaskConstants.TASK_REDIS_SSL, false);
        this.authUser = profile.get(TaskConstants.TASK_REDIS_AUTHUSER, "");
        this.authPassword = profile.get(TaskConstants.TASK_REDIS_AUTHPASSWORD, "");
        this.readTimeout = profile.get(TaskConstants.TASK_REDIS_READTIMEOUT, "");
        this.replId = profile.get(TaskConstants.TASK_REDIS_REPLID, "");
        this.snapShot = profile.get(TaskConstants.TASK_REDIS_OFFSET, "-1");
        this.dbName = profile.get(TaskConstants.TASK_REDIS_DB_NAME);
        this.keys = new ConcurrentSkipListSet<>(Arrays.asList(profile.get(TaskConstants.TASK_REDIS_KEYS).split(",")));
        this.isSubscribe = profile.getBoolean(TaskConstants.TASK_REDIS_IS_SUBSCRIBE, false);
        this.instanceId = profile.getInstanceId();
        this.redisQueue = new LinkedBlockingQueue<>(REDIS_QUEUE_SIZE);
        initGson();
        String uri = getRedisUri();
        try {
            if (isSubscribe) {
                // use subscribe mode
                this.subscribeOperations = new ConcurrentSkipListSet<>(
                        Arrays.asList(profile.get(TaskConstants.TASK_REDIS_SUBSCRIPTION_OPERATION).split(",")));
                this.executor = (ScheduledExecutorService) Executors.newSingleThreadExecutor();
                this.redisReplicator = new RedisReplicator(uri);
                initReplicator();
                this.executor.execute(startReplicatorSync());
            } else {
                this.executor = Executors.newScheduledThreadPool(1);
                // use command mode
                this.redisCommand = profile.get(TaskConstants.TASK_REDIS_COMMAND, GET_COMMAND);
                this.fieldOrMember = profile.get(TaskConstants.TASK_REDIS_FIELD_OR_MEMBER, null);
                // default frequency 1min
                long syncFreq = profile.getLong(TaskConstants.TASK_REDIS_SYNC_FREQ, DEFAULT_FREQ);
                this.jedis = new Jedis(uri);
                jedis.connect();
                this.executor.scheduleWithFixedDelay(startJedisSync(), 0, syncFreq, TimeUnit.MILLISECONDS);
            }
        } catch (URISyntaxException | IOException | JedisConnectionException e) {
            sourceMetric.pluginReadFailCount.addAndGet(1);
            LOGGER.error("Connect to redis {}:{} failed.", hostName, port, e);
        }
    }

    private Runnable startReplicatorSync() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName() + "redis subscribe mode");
            executor.execute(new Thread(() -> {
                try {
                    this.redisReplicator.open();
                } catch (IOException e) {
                    LOGGER.error("Redis source error, fail to start replicator", e);
                }
            }));
        };
    }

    private Runnable startJedisSync() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName() + "redis command mode");
            executor.execute(new Thread(() -> {
                Map<String, Object> dataMap =
                        fetchDataByJedis(jedis, redisCommand, new ArrayList<>(keys), fieldOrMember);
                synchronizeData(gson.toJson(dataMap));
            }));
        };
    }

    private Map<String, Object> fetchDataByJedis(Jedis jedis, String command, List<String> keys, String fieldOrMember) {
        Map<String, Object> result = new HashMap<>();
        CommandHandler handler = commandHandlers.get(command.toUpperCase());
        if (handler != null) {
            handler.handle(jedis, keys, fieldOrMember, result);
        } else {
            LOGGER.error("Unsupported command: " + command);
            throw new UnsupportedOperationException("Unsupported command: " + command);
        }
        return result;
    }

    private static void handleGet(Jedis jedis, List<String> keys, String fieldOrMember, Map<String, Object> result) {
        Pipeline pipeline = jedis.pipelined();
        for (String key : keys) {
            pipeline.get(key);
        }
        List<Object> getValues = pipeline.syncAndReturnAll();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), getValues.get(i));
        }
    }

    private static void handleMGet(Jedis jedis, List<String> keys, String fieldOrMember, Map<String, Object> result) {
        List<String> mGetValues = jedis.mget(keys.toArray(new String[0]));
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), mGetValues.get(i));
        }
    }

    private static void handleHGet(Jedis jedis, List<String> keys, String fieldOrMember, Map<String, Object> result) {
        for (String key : keys) {
            String value = jedis.hget(key, fieldOrMember);
            result.put(key, value);
        }
    }

    private static void handleZScore(Jedis jedis, List<String> keys, String fieldOrMember, Map<String, Object> result) {
        for (String key : keys) {
            if (!StringUtils.isEmpty(fieldOrMember)) {
                Double score = jedis.zscore(key, fieldOrMember);
                result.put(key, score);
            }
        }
    }

    private static void handleZRevRank(Jedis jedis, List<String> keys, String fieldOrMember,
            Map<String, Object> result) {
        for (String key : keys) {
            if (!StringUtils.isEmpty(fieldOrMember)) {
                Long rank = jedis.zrevrank(key, fieldOrMember);
                result.put(key, rank);
            }
        }
    }

    private static void handleExists(Jedis jedis, List<String> keys, String fieldOrMember, Map<String, Object> result) {
        for (String key : keys) {
            boolean exists = jedis.exists(key);
            result.put(key, exists);
        }
    }

    // Functional interface for handling commands
    @FunctionalInterface
    private interface CommandHandler {

        void handle(Jedis jedis, List<String> keys, String fieldOrMember, Map<String, Object> result);
    }

    private void synchronizeData(String data) {
        try {
            if (!StringUtils.isEmpty(data)) {
                byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
                // limit data size
                if (dataBytes.length <= MAX_DATA_SIZE) {
                    SourceData sourceData = new SourceData(dataBytes, "0L");
                    boolean offerSuc = false;
                    while (isRunnable() && !offerSuc) {
                        offerSuc = redisQueue.offer(sourceData, 1, TimeUnit.SECONDS);
                    }
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                            System.currentTimeMillis(), 1, data.length());
                    sourceMetric.pluginReadCount.incrementAndGet();
                } else {
                    sourceMetric.pluginReadFailCount.incrementAndGet();
                    LOGGER.warn("Read redis data warn, data overload, Automatically skip and discard");
                }
            }
        } catch (InterruptedException e) {
            sourceMetric.pluginReadFailCount.incrementAndGet();
            LOGGER.error("Read redis data error", e);
        }
    }

    @Override
    protected void printCurrentState() {
        if (isSubscribe) {
            LOGGER.info("redis subscribe synchronization is {} on source {}",
                    redisReplicator != null && !executor.isShutdown() ? "running" : "stop",
                    hostName + ":" + port);
        } else {
            LOGGER.info("redis command synchronization is {} on source {}", !executor.isShutdown() ? "running" : "stop",
                    hostName + ":" + port);
        }
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        try {
            int size = 0;
            while (size < BATCH_READ_LINE_TOTAL_LEN) {
                SourceData sourceData = redisQueue.poll(1, TimeUnit.SECONDS);
                if (sourceData != null) {
                    size += sourceData.getData().length;
                    dataList.add(sourceData);
                } else {
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("poll {} data from redis queue interrupted.", instanceId);
        }
        return dataList;
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        LOGGER.info("releasing redis source");
        if (!destroyed) {
            try {
                executor.shutdown();
                // subscribe mode then close replicator
                if (redisReplicator != null) {
                    redisReplicator.close();
                }
                // command mode then close jedis
                if (jedis.isConnected()) {
                    jedis.close();
                }
            } catch (IOException e) {
                LOGGER.error("Redis reader close failed.");
            }
            destroyed = true;
        }
    }

    @Override
    public boolean sourceFinish() {
        return false;
    }

    @Override
    public boolean sourceExist() {
        return true;
    }

    private String getRedisUri() {
        StringBuffer sb = new StringBuffer("redis://");
        sb.append(hostName).append(":").append(port);
        if (!StringUtils.isEmpty(dbName)) {
            sb.append("/").append(dbName);
        }
        sb.append("?");
        if (!StringUtils.isEmpty(authPassword)) {
            sb.append("authPassword=").append(authPassword).append("&");
        }
        if (!StringUtils.isEmpty(authUser)) {
            sb.append("authUser=").append(authUser).append("&");
        }
        if (!StringUtils.isEmpty(readTimeout)) {
            sb.append("readTimeout=").append(readTimeout).append("&");
        }
        if (ssl) {
            sb.append("ssl=").append("yes").append("&");
        }
        if (!StringUtils.isEmpty(snapShot)) {
            sb.append("replOffset=").append(snapShot).append("&");
        }
        if (!StringUtils.isEmpty(replId)) {
            sb.append("replId=").append(replId).append("&");
        }
        if (sb.charAt(sb.length() - 1) == '?' || sb.charAt(sb.length() - 1) == '&') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    private void initReplicator() {
        if (!subscribeOperations.isEmpty()) {
            DefaultCommandParser replicatorCommandParser = new DefaultCommandParser();
            for (String subOperation : subscribeOperations) {
                this.redisReplicator.addCommandParser(CommandName.name(subOperation), replicatorCommandParser);
            }
            this.redisReplicator.addEventListener((replicator, event) -> {
                if (event instanceof DefaultCommand) {
                    DefaultCommand defaultCommand = (DefaultCommand) event;
                    Object[] args = defaultCommand.getArgs();
                    if (args[0] instanceof byte[]) {
                        String key = new String((byte[]) args[0], StandardCharsets.UTF_8);
                        if (keys.contains(key)) {
                            synchronizeData(gson.toJson(event));
                        }
                    }
                }
                if (event instanceof PostRdbSyncEvent) {
                    this.snapShot = String.valueOf(replicator.getConfiguration().getReplOffset());
                    LOGGER.info("after rdb snapShot is: {}", snapShot);
                }
            });
        } else {
            // if SubOperation is not configured, subscribe all modification
            initDefaultReplicator();
        }
    }

    private void initDefaultReplicator() {
        DefaultCommandParser defaultCommandParser = new DefaultCommandParser();
        this.redisReplicator.addCommandParser(CommandName.name("APPEND"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SET"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SETEX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("MSET"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("DEL"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SADD"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("HMSET"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("HSET"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LSET"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("EXPIRE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("EXPIREAT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("GETSET"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("HSETNX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PSETEX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SETNX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SETRANGE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("HDEL"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LPOP"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LPUSH"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LPUSHX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LRem"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RPOP"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RPUSH"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RPUSHX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZREM"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RENAME"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("INCR"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("DECR"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("INCRBY"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("DECRBY"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PERSIST"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SELECT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("FLUSHALL"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("FLUSHDB"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("HINCRBY"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZINCRBY"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("MOVE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SMOVE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PFADD"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PFCOUNT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PFMERGE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SDIFFSTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SINTERSTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SUNIONSTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZADD"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZINTERSTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZUNIONSTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("BRPOPLPUSH"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LINSERT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RENAMENX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RESTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PEXPIRE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PEXPIREAT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("GEOADD"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("EVAL"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("EVALSHA"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SCRIPT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("PUBLISH"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("BITOP"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("BITFIELD"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SETBIT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SREM"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("UNLINK"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SWAPDB"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("MULTI"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("EXEC"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZREMRANGEBYSCORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZREMRANGEBYRANK"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZREMRANGEBYLEX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LTRIM"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("SORT"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("RPOPLPUSH"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZPOPMIN"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZPOPMAX"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("REPLCONF"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XACK"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XADD"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XCLAIM"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XDEL"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XGROUP"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XTRIM"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("XSETID"), defaultCommandParser);
        // since redis 6.2
        this.redisReplicator.addCommandParser(CommandName.name("COPY"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("LMOVE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("BLMOVE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("ZDIFFSTORE"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("GEOSEARCHSTORE"), defaultCommandParser);
        // since redis 7.0
        this.redisReplicator.addCommandParser(CommandName.name("SPUBLISH"), defaultCommandParser);
        this.redisReplicator.addCommandParser(CommandName.name("FUNCTION"), defaultCommandParser);
        // add EventListener
        this.redisReplicator.addEventListener((replicator, event) -> {
            if (event instanceof KeyValuePair<?, ?> || event instanceof DefaultCommand) {
                KeyValuePair<?, ?> kvEvent = (KeyValuePair<?, ?>) event;
                String key = kvEvent.getKey().toString();
                if (keys.contains(key)) {
                    synchronizeData(gson.toJson(event));
                }
            }
            if (event instanceof PostRdbSyncEvent) {
                this.snapShot = String.valueOf(replicator.getConfiguration().getReplOffset());
                LOGGER.info("after rdb snapShot is: {}", snapShot);
            }
        });
    }

    /**
     * init GSON parser
     */
    private void initGson() {
        this.gson =
                new GsonBuilder().registerTypeAdapter(KeyStringValueHash.class, new TypeAdapter<KeyStringValueHash>() {

                    @Override
                    public void write(JsonWriter out, KeyStringValueHash kv) throws IOException {
                        out.beginObject();
                        out.name("DB").beginObject();
                        out.name("dbNumber").value(kv.getDb().getDbNumber());
                        out.name("dbSize").value(kv.getDb().getDbsize());
                        out.name("expires").value(kv.getDb().getExpires());
                        out.endObject();
                        out.name("valueRdbType").value(kv.getValueRdbType());
                        out.name("key").value(new String(kv.getKey()));
                        out.name("value").beginObject();
                        for (byte[] b : kv.getValue().keySet()) {
                            out.name(new String(b)).value(new String(kv.getValue().get(b)));
                        }
                        out.endObject();
                        out.endObject();
                    }

                    @Override
                    public KeyStringValueHash read(JsonReader in) throws IOException {
                        return null;
                    }
                }).registerTypeAdapter(DefaultCommand.class, new TypeAdapter<DefaultCommand>() {

                    @Override
                    public void write(JsonWriter out, DefaultCommand dc) throws IOException {
                        out.beginObject();
                        out.name("key").value(new String(dc.getCommand()));
                        out.name("value").beginArray();
                        for (byte[] bytes : dc.getArgs()) {
                            out.value(new String(bytes));
                        }
                        out.endArray();
                        out.endObject();
                    }

                    @Override
                    public DefaultCommand read(JsonReader in) throws IOException {
                        return null;
                    }
                })
                        .registerTypeAdapter(KeyStringValueList.class, new TypeAdapter<KeyStringValueList>() {

                            @Override
                            public void write(JsonWriter out, KeyStringValueList kv) throws IOException {
                                out.beginObject();
                                out.name("key").value(new String(kv.getKey()));
                                out.name("value").beginArray();
                                for (byte[] bytes : kv.getValue()) {
                                    out.value(new String(bytes));
                                }
                                out.endArray();
                                out.endObject();
                            }

                            @Override
                            public KeyStringValueList read(JsonReader in) throws IOException {
                                return null;
                            }
                        })
                        .registerTypeAdapter(KeyStringValueSet.class, new TypeAdapter<KeyStringValueSet>() {

                            @Override
                            public void write(JsonWriter out, KeyStringValueSet kv) throws IOException {
                                out.beginObject();
                                out.name("key").value(new String(kv.getKey()));
                                out.name("value").beginArray();
                                for (byte[] bytes : kv.getValue()) {
                                    out.value(new String(bytes));
                                }
                                out.endArray();
                                out.endObject();
                            }

                            @Override
                            public KeyStringValueSet read(JsonReader in) throws IOException {
                                return null;
                            }
                        })
                        .registerTypeAdapter(KeyStringValueString.class, new TypeAdapter<KeyStringValueString>() {

                            @Override
                            public void write(JsonWriter out, KeyStringValueString kv) throws IOException {
                                out.beginObject();
                                out.name("key").value(new String(kv.getKey()));
                                out.name("value").value(new String(kv.getValue()));
                                out.endObject();
                            }

                            @Override
                            public KeyStringValueString read(JsonReader in) throws IOException {
                                return null;
                            }
                        })
                        .registerTypeAdapter(KeyStringValueZSet.class, new TypeAdapter<KeyStringValueZSet>() {

                            @Override
                            public void write(JsonWriter out, KeyStringValueZSet kv) throws IOException {
                                out.beginObject();
                                out.name("key").value(new String(kv.getKey()));
                                out.name("value").beginArray();
                                for (ZSetEntry entry : kv.getValue()) {
                                    out.beginObject();
                                    out.name("element").value(new String(entry.getElement()));
                                    out.name("score").value(entry.getScore());
                                    out.endObject();
                                }
                                out.endArray();
                                out.endObject();
                            }

                            @Override
                            public KeyStringValueZSet read(JsonReader in) throws IOException {
                                return null;
                            }
                        })
                        .create();
    }
}
