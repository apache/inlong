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

import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Redis source
 */
public class RedisSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSource.class);
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
    private String dbNumber;
    private String redisCommand;

    private String fieldOrMember;
    private boolean destroyed;

    private Set<String> keys;
    private Replicator redisReplicator;
    private BlockingQueue<SourceData> redisQueue;
    private ExecutorService executor;

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
        this.dbNumber = profile.get(TaskConstants.TASK_REDIS_DB_NUMBER, "0");
        this.redisCommand = profile.get(TaskConstants.TASK_REDIS_COMMAND, "get");
        this.keys = new ConcurrentSkipListSet<>(Arrays.asList(profile.get(TaskConstants.TASK_REDIS_KEYS).split(",")));
        this.fieldOrMember = profile.get(TaskConstants.TASK_REDIS_FIELD_OR_NUMBER, null);
        this.instanceId = profile.getInstanceId();
        this.redisQueue = new LinkedBlockingQueue<>(profile.getInt(TaskConstants.TASK_REDIS_QUEUE_SIZE, 10000));
        String uri = getRedisUri();
        try {
            redisReplicator = new RedisReplicator(uri);
            this.jedis = new Jedis(uri);
            initReplicator();
            executor = Executors.newSingleThreadExecutor();
            executor.execute(startRedisReplicator());
        } catch (URISyntaxException | IOException e) {
            sourceMetric.pluginReadFailCount.addAndGet(1);
            LOGGER.error("Connect to redis {}:{} failed.", hostName, port);
        }
    }

    private Runnable startRedisReplicator() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName() + "redis replicator");
            executor.execute(new Thread(() -> {
                try {
                    this.redisReplicator.open();
                } catch (IOException e) {
                    LOGGER.error("Redis source error", e);
                }
            }));
        };
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("redis replicator is {} on source {}", redisReplicator != null ? "running" : "free",
                hostName + ":" + port);
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
                redisReplicator.close();
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
        if (!StringUtils.isEmpty(dbNumber)) {
            sb.append("/").append(dbNumber);
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
        redisReplicator.addEventListener((replicator, event) -> {
            try {
                if (event instanceof KeyValuePair<?, ?>) {
                    KeyValuePair<?, ?> kvEvent = (KeyValuePair<?, ?>) event;
                    String key = kvEvent.getKey().toString();
                    if (keys.contains(key)) {
                        String data = jedisExecuteCommand(redisCommand, key, fieldOrMember);
                        if (!StringUtils.isEmpty(data)) {
                            SourceData sourceData = new SourceData(data.getBytes(StandardCharsets.UTF_8), "0L");
                            boolean offerSuc = false;
                            while (isRunnable() && !offerSuc) {
                                offerSuc = redisQueue.offer(sourceData, 1, TimeUnit.SECONDS);
                            }
                            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                                    System.currentTimeMillis(), 1, data.length());
                            sourceMetric.pluginReadCount.incrementAndGet();
                        }
                    }
                }
                if (event instanceof PostRdbSyncEvent) {
                    this.snapShot = String.valueOf(replicator.getConfiguration().getReplOffset());
                    LOGGER.info("after rdb snapShot is: {}", snapShot);
                }
            } catch (InterruptedException e) {
                sourceMetric.pluginReadFailCount.incrementAndGet();
                LOGGER.error("Read redis data error", e);
            }
        });
    }

    private String jedisExecuteCommand(String command, String key, String fieldOrMember) {
        switch (command.toUpperCase()) {
            case "GET":
                return jedis.get(key);
            case "HGET":
                if (!StringUtils.isEmpty(fieldOrMember)) {
                    return jedis.hget(key, fieldOrMember);
                }
                break;
            case "ZSCORE":
                if (!StringUtils.isEmpty(fieldOrMember)) {
                    Double score = jedis.zscore(key, fieldOrMember);
                    return score != null ? score.toString() : null;
                }
                break;
            case "ZREVRANK":
                if (StringUtils.isEmpty(fieldOrMember)) {
                    Long rank = jedis.zrevrank(key, fieldOrMember);
                    return rank != null ? rank.toString() : null;
                }
                break;
            default:
                LOGGER.info("Unsupported command: " + command);
        }
        return null;
    }

}
