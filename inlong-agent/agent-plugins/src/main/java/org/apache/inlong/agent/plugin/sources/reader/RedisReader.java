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

package org.apache.inlong.agent.plugin.sources.reader;

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
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueHash;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueList;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueSet;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueZSet;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Redis data reader
 */
public class RedisReader extends AbstractReader {

    public static final String REDIS_READER_TAG_NAME = "AgentRedisMetric";
    public static final String JOB_REDIS_PORT = "job.redisJob.port";
    public static final String JOB_REDIS_HOSTNAME = "job.redisJob.hostname";
    public static final String JOB_REDIS_SSL = "job.redisJob.ssl";
    public static final String JOB_REDIS_AUTHUSER = "job.redisJob.authUser";
    public static final String JOB_REDIS_AUTHPASSWORD = "job.redisJob.authPassword";
    public static final String JOB_REDIS_READTIMEOUT = "job.redisJob.readTimeout";
    public static final String JOB_REDIS_QUEUE_SIZE = "job.redisJob.queueSize";
    public static final String JOB_REDIS_REPLID = "job.redisJob.replId";
    public static final String JOB_REDIS_OFFSET = "job.redisJob.offset";
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisReader.class);
    private String port;
    private String hostName;
    private boolean ssl;
    private String authUser;
    private String authPassword;
    private String readTimeout;
    private String instanceId;
    private String replId;
    private String snapShot;
    private boolean destroyed;
    private Replicator redisReplicator;
    private LinkedBlockingQueue<String> redisMessageQueue;
    private boolean finished = false;
    private ExecutorService executor;
    private Gson GSON;


    @Override
    public void init(JobProfile jobConf) {
        super.init(jobConf);
        LOGGER.info("Init redis reader with jobConf {}", jobConf.toJsonStr());
        port = jobConf.get(JOB_REDIS_PORT);
        hostName = jobConf.get(JOB_REDIS_HOSTNAME);
        ssl = jobConf.getBoolean(JOB_REDIS_SSL, false);
        authUser = jobConf.get(JOB_REDIS_AUTHUSER, "");
        authPassword = jobConf.get(JOB_REDIS_AUTHPASSWORD, "");
        readTimeout = jobConf.get(JOB_REDIS_READTIMEOUT, "");
        replId = jobConf.get(JOB_REDIS_REPLID, "");
        snapShot = jobConf.get(JOB_REDIS_OFFSET, "-1");
        instanceId = jobConf.getInstanceId();
        finished = false;
        redisMessageQueue = new LinkedBlockingQueue<>(jobConf.getInt(JOB_REDIS_QUEUE_SIZE, 10000));
        initGson();
        String uri = getRedisUri();
        try {
            redisReplicator = new RedisReplicator(uri);
            initReplicator();
            redisReplicator.addEventListener(new EventListener() {
                @Override
                public void onEvent(Replicator replicator, Event event) {
                    try {
                        if (event instanceof DefaultCommand || event instanceof KeyValuePair<?, ?>) {
                            redisMessageQueue.put(GSON.toJson(event));
                            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                                    System.currentTimeMillis(), 1);
                            readerMetric.pluginReadCount.incrementAndGet();
                        }
                        if (event instanceof PostRdbSyncEvent) {
                            snapShot = String.valueOf(replicator.getConfiguration().getReplOffset());
                            LOGGER.info("after rdb snapShot is: {}", snapShot);
                        }
                    } catch (InterruptedException e) {
                        readerMetric.pluginReadFailCount.incrementAndGet();
                        LOGGER.error("Read redis data error", e);
                    }
                }
            });
            executor = Executors.newSingleThreadExecutor();
            executor.execute(new Thread(() -> {
                try {
                    redisReplicator.open();
                } catch (IOException e) {
                    LOGGER.error("Redis source error", e);
                }
            }));
        } catch (URISyntaxException | IOException e) {
            readerMetric.pluginReadFailCount.addAndGet(1);
            LOGGER.error("Connect to redis {}:{} failed.", hostName, port);
        }
    }

    private String getRedisUri() {
        StringBuffer sb = new StringBuffer("redis://");
        sb.append(hostName).append(":").append(port);
        sb.append("?");
        if (authPassword != null && !authPassword.equals("")) {
            sb.append("authPassword=").append(authPassword).append("&");
        }
        if (authUser != null && !authUser.equals("")) {
            sb.append("authUser=").append(authUser).append("&");
        }
        if (readTimeout != null && !readTimeout.equals("")) {
            sb.append("readTimeout=").append(readTimeout).append("&");
        }
        if (ssl) {
            sb.append("ssl=").append("yes").append("&");
        }
        if (snapShot != null && !snapShot.equals("")) {
            sb.append("replOffset=").append(snapShot).append("&");
        }
        if (replId != null && !replId.equals("")) {
            sb.append("replId=").append(replId).append("&");
        }
        if (sb.charAt(sb.length() - 1) == '?' || sb.charAt(sb.length() - 1) == '&') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public void destroy() {
        synchronized (this) {
            if (!destroyed) {
                try {
                    executor.shutdown();
                    redisReplicator.close();
                } catch (IOException e) {
                    LOGGER.error("Redis reader close failed.");
                }
                destroyed = true;
            }
        }
    }


    @Override
    public Message read() {
        if (!redisMessageQueue.isEmpty()) {
            readerMetric.pluginReadCount.incrementAndGet();
            return new DefaultMessage(redisMessageQueue.poll().getBytes());
        } else {
            return null;
        }
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public String getReadSource() {
        return instanceId;
    }

    @Override
    public void setReadTimeout(long mill) {

    }

    @Override
    public void setWaitMillisecond(long millis) {

    }

    @Override
    public String getSnapshot() {
        return snapShot;
    }

    @Override
    public void finishRead() {
        finished = true;
    }

    @Override
    public boolean isSourceExist() {
        return true;
    }

    /**
     * init GSON parser
     */
    private void initGson() {
        GSON = new GsonBuilder().registerTypeAdapter(KeyStringValueHash.class, new TypeAdapter<KeyStringValueHash>() {

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

    /**
     * init replicator's commandParser
     */
    private void initReplicator() {
        redisReplicator.addCommandParser(CommandName.name("APPEND"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SET"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SETEX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("MSET"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("DEL"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SADD"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("HMSET"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("HSET"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LSET"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("EXPIRE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("EXPIREAT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("GETSET"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("HSETNX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("MSETNX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PSETEX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SETNX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SETRANGE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("HDEL"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LPOP"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LPUSH"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LPUSHX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LRem"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RPOP"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RPUSH"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RPUSHX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZREM"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RENAME"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("INCR"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("DECR"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("INCRBY"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("DECRBY"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PERSIST"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SELECT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("FLUSHALL"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("FLUSHDB"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("HINCRBY"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZINCRBY"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("MOVE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SMOVE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PFADD"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PFCOUNT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PFMERGE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SDIFFSTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SINTERSTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SUNIONSTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZADD"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZINTERSTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZUNIONSTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("BRPOPLPUSH"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LINSERT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RENAMENX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RESTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PEXPIRE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PEXPIREAT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("GEOADD"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("EVAL"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("EVALSHA"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SCRIPT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("PUBLISH"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("BITOP"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("BITFIELD"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SETBIT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SREM"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("UNLINK"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SWAPDB"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("MULTI"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("EXEC"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZREMRANGEBYSCORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZREMRANGEBYRANK"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZREMRANGEBYLEX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LTRIM"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("SORT"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("RPOPLPUSH"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZPOPMIN"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZPOPMAX"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("REPLCONF"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XACK"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XADD"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XCLAIM"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XDEL"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XGROUP"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XTRIM"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("XSETID"), new DefaultCommandParser());
        // since redis 6.2
        redisReplicator.addCommandParser(CommandName.name("COPY"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("LMOVE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("BLMOVE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("ZDIFFSTORE"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("GEOSEARCHSTORE"), new DefaultCommandParser());
        // since redis 7.0
        redisReplicator.addCommandParser(CommandName.name("SPUBLISH"), new DefaultCommandParser());
        redisReplicator.addCommandParser(CommandName.name("FUNCTION"), new DefaultCommandParser());
    }
}
