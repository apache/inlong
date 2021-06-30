/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.common.offsetstorage;

import java.net.BindException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.inlong.tubemq.corebase.TokenConstants;
import org.apache.inlong.tubemq.server.broker.exception.OffsetStoreException;
import org.apache.inlong.tubemq.server.common.TServerConstants;
import org.apache.inlong.tubemq.server.common.fileconfig.ZKConfig;
import org.apache.inlong.tubemq.server.common.offsetstorage.zookeeper.ZKUtil;
import org.apache.inlong.tubemq.server.common.offsetstorage.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A offset storage implementation with zookeeper
 */
public class ZkOffsetStorage implements OffsetStorage {
    private static final Logger logger = LoggerFactory.getLogger(ZkOffsetStorage.class);

    static {
        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    if (e instanceof BindException) {
                        logger.error("Bind failed.", e);
                        // System.exit(1);
                    }
                    if (e instanceof IllegalStateException
                            && e.getMessage().contains("Shutdown in progress")) {
                        return;
                    }
                    logger.warn("Thread terminated with exception: " + t.getName(), e);
                }
            });
        }
    }

    private final String tubeZkRoot;
    private final String consumerZkDir;
    private final boolean isBroker;
    private final int brokerId;
    private final String strBrokerId;
    private ZKConfig zkConfig;
    private ZooKeeperWatcher zkw;


    public ZkOffsetStorage(final ZKConfig zkConfig, boolean isBroker, int brokerId) {
        this.zkConfig = zkConfig;
        this.isBroker = isBroker;
        this.brokerId = brokerId;
        this.strBrokerId = String.valueOf(brokerId);
        this.tubeZkRoot = normalize(this.zkConfig.getZkNodeRoot());
        this.consumerZkDir = this.tubeZkRoot + "/consumers-v3";
        try {
            this.zkw = new ZooKeeperWatcher(zkConfig);
        } catch (Throwable e) {
            logger.error(new StringBuilder(256)
                    .append("[ZkOffsetStorage] Failed to connect ZooKeeper server (")
                    .append(this.zkConfig.getZkServerAddr()).append(") !").toString(), e);
            System.exit(1);
        }
        logger.info("[ZkOffsetStorage] ZooKeeper Offset Storage initiated!");
    }

    @Override
    public void close() {
        if (this.zkw != null) {
            logger.info("ZooKeeper Offset Storage closing .......");
            this.zkw.close();
            this.zkw = null;
            logger.info("ZooKeeper Offset Storage closed!");
        }
    }

    @Override
    public void commitOffset(final String group,
                             final Collection<OffsetStorageInfo> offsetInfoList,
                             boolean isFailRetry) {
        if (this.zkw == null
                || offsetInfoList == null
                || offsetInfoList.isEmpty()) {
            return;
        }
        StringBuilder sBuilder = new StringBuilder(512);
        if (isFailRetry) {
            for (int i = 0; i < TServerConstants.CFG_ZK_COMMIT_DEFAULT_RETRIES; i++) {
                try {
                    cfmOffset(sBuilder, group, offsetInfoList);
                    break;
                } catch (Exception e) {
                    logger.error("Error found when commit offsets to ZooKeeper with retry " + i, e);
                    try {
                        Thread.sleep(this.zkConfig.getZkSyncTimeMs());
                    } catch (InterruptedException ie) {
                        logger.error(
                                "InterruptedException when commit offset to ZooKeeper with retry " + i, ie);
                        return;
                    }
                }
            }
        } else {
            try {
                cfmOffset(sBuilder, group, offsetInfoList);
            } catch (OffsetStoreException e) {
                logger.error("Error when commit offsets to ZooKeeper", e);
            }
        }
    }

    @Override
    public OffsetStorageInfo loadOffset(final String group, final String topic, int partitionId) {
        String znode = new StringBuilder(512).append(this.consumerZkDir).append("/")
                .append(group).append("/offsets/").append(topic).append("/")
                .append(brokerId).append(TokenConstants.HYPHEN)
                .append(partitionId).toString();
        String offsetZkInfo;
        try {
            offsetZkInfo = ZKUtil.readDataMaybeNull(this.zkw, znode);
        } catch (KeeperException e) {
            logger.error("KeeperException during load offsets from ZooKeeper", e);
            return null;
        }
        if (offsetZkInfo == null) {
            return null;
        }
        String[] offsetInfoStrs =
                offsetZkInfo.split(TokenConstants.HYPHEN);
        return new OffsetStorageInfo(topic, brokerId, partitionId,
                Long.parseLong(offsetInfoStrs[1]), Long.parseLong(offsetInfoStrs[0]), false);

    }

    private void cfmOffset(final StringBuilder sb, final String group,
                           final Collection<OffsetStorageInfo> infoList) throws OffsetStoreException {
        sb.delete(0, sb.length());
        for (final OffsetStorageInfo info : infoList) {
            long newOffset = -1;
            long msgId = -1;
            synchronized (info) {
                if (!info.isModified()) {
                    continue;
                }
                newOffset = info.getOffset();
                msgId = info.getMessageId();
                info.setModified(false);
            }
            final String topic = info.getTopic();
            String offsetPath = sb.append(this.consumerZkDir).append("/")
                    .append(group).append("/offsets/").append(topic).append("/")
                    .append(info.getBrokerId()).append(TokenConstants.HYPHEN)
                    .append(info.getPartitionId()).toString();
            sb.delete(0, sb.length());
            String offsetData =
                    sb.append(msgId).append(TokenConstants.HYPHEN).append(newOffset).toString();
            sb.delete(0, sb.length());
            try {
                ZKUtil.updatePersistentPath(this.zkw, offsetPath, offsetData);
            } catch (final Throwable t) {
                logger.error("Exception during commit offsets to ZooKeeper", t);
                throw new OffsetStoreException(t);
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sb.append("Committed offset, path=")
                        .append(offsetPath).append(", data=").append(offsetData).toString());
                sb.delete(0, sb.length());
            }
        }
    }

    /**
     * Get offset stored in zookeeper, if not found or error, set null
     * <p/>
     *
     * @return partitionId--offset map info
     */
    @Override
    public Map<Integer, Long> queryGroupOffsetInfo(String group, String topic,
                                                  Set<Integer> partitionIds) {
        StringBuilder sBuider = new StringBuilder(512);
        String basePath = sBuider.append(this.consumerZkDir).append("/")
                .append(group).append("/offsets/").append(topic).append("/")
                .append(brokerId).append(TokenConstants.HYPHEN).toString();
        sBuider.delete(0, sBuider.length());
        String offsetZkInfo = null;
        Map<Integer, Long> offsetMap = new HashMap<>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            String offsetNode = sBuider.append(basePath).append(partitionId).toString();
            sBuider.delete(0, sBuider.length());
            try {
                offsetZkInfo = ZKUtil.readDataMaybeNull(this.zkw, offsetNode);
                if (offsetZkInfo == null) {
                    offsetMap.put(partitionId, null);
                } else {
                    String[] offsetInfoStrs =
                            offsetZkInfo.split(TokenConstants.HYPHEN);
                    offsetMap.put(partitionId, Long.parseLong(offsetInfoStrs[1]));
                }
            } catch (Throwable e) {
                offsetMap.put(partitionId, null);
            }
        }
        return offsetMap;
    }

    /**
     * Query booked topic info of groups stored in zookeeper.
     * @param groupSet query groups
     * @return group--topic map info
     */
    @Override
    public Map<String, Set<String>> queryZKGroupTopicInfo(List<String> groupSet) {
        String qryBrokerId;
        Map<String, Set<String>> groupTopicMap = new HashMap<>();
        StringBuilder sBuider = new StringBuilder(512);
        if (groupSet == null || groupSet.isEmpty()) {
            return groupTopicMap;
        }
        // build path base
        String groupNode = sBuider.append(this.consumerZkDir).toString();
        sBuider.delete(0, sBuider.length());
        // get the group managed by this broker
        for (String group : groupSet) {
            String topicNode = sBuider.append(groupNode)
                    .append("/").append(group).append("/offsets").toString();
            List<String> consumeTopics = ZKUtil.getChildren(this.zkw, topicNode);
            sBuider.delete(0, sBuider.length());
            Set<String> topicSet = new HashSet<>();
            if (consumeTopics != null) {
                for (String topic : consumeTopics) {
                    if (topic == null) {
                        continue;
                    }
                    String brokerNode = sBuider.append(topicNode)
                            .append("/").append(topic).toString();
                    List<String> brokerIds = ZKUtil.getChildren(this.zkw, brokerNode);
                    sBuider.delete(0, sBuider.length());
                    if (brokerIds != null) {
                        for (String idStr : brokerIds) {
                            if (idStr != null) {
                                String[] brokerPartIdStrs =
                                        idStr.split(TokenConstants.HYPHEN);
                                qryBrokerId = brokerPartIdStrs[0];
                                if (qryBrokerId != null
                                        && strBrokerId.equals(qryBrokerId.trim())) {
                                    topicSet.add(topic);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if (!topicSet.isEmpty()) {
                groupTopicMap.put(group, topicSet);
            }
        }
        return groupTopicMap;
    }

    /**
     * Get group-topic map info stored in zookeeper.
     * <p/>
     * The broker only cares about the content of its own node
     *
     */
    @Override
    public Map<String, Set<String>> queryZkAllGroupTopicInfos() {
        StringBuilder sBuider = new StringBuilder(512);
        // get all booked groups name
        String groupNode = sBuider.append(this.consumerZkDir).toString();
        List<String> bookedGroups = ZKUtil.getChildren(this.zkw, groupNode);
        return queryZKGroupTopicInfo(bookedGroups);
    }

    /**
     * Get offset stored in zookeeper, if not found or error, set null
     * <p/>
     *
     * @return partitionId--offset map info
     */
    @Override
    public void deleteGroupOffsetInfo(
            Map<String, Map<String, Set<Integer>>> groupTopicPartMap) {
        StringBuilder sBuider = new StringBuilder(512);
        for (Map.Entry<String, Map<String, Set<Integer>>> entry
                : groupTopicPartMap.entrySet()) {
            if (entry.getKey() == null
                    || entry.getValue() == null
                    || entry.getValue().isEmpty()) {
                continue;
            }
            String basePath = sBuider.append(this.consumerZkDir).append("/")
                    .append(entry.getKey()).append("/offsets").toString();
            sBuider.delete(0, sBuider.length());
            Map<String, Set<Integer>> topicPartMap = entry.getValue();
            for (Map.Entry<String, Set<Integer>> topicEntry : topicPartMap.entrySet()) {
                if (topicEntry.getKey() == null
                        || topicEntry.getValue() == null
                        || topicEntry.getValue().isEmpty()) {
                    continue;
                }
                Set<Integer> partIdSet = topicEntry.getValue();
                for (Integer partitionId : partIdSet) {
                    String offsetNode = sBuider.append(basePath).append("/")
                            .append(topicEntry.getKey()).append("/")
                            .append(brokerId).append(TokenConstants.HYPHEN)
                            .append(partitionId).toString();
                    sBuider.delete(0, sBuider.length());
                    ZKUtil.delZNode(this.zkw, offsetNode);
                }
                String parentNode = sBuider.append(basePath).append("/")
                        .append(topicEntry.getKey()).toString();
                sBuider.delete(0, sBuider.length());
                chkAndRmvBlankParentNode(parentNode);
            }
            chkAndRmvBlankParentNode(basePath);
            String parentNode = sBuider.append(this.consumerZkDir)
                    .append("/").append(entry.getKey()).toString();
            sBuider.delete(0, sBuider.length());
            chkAndRmvBlankParentNode(parentNode);
        }
    }

    private void chkAndRmvBlankParentNode(String parentNode) {
        List<String> nodeSet = ZKUtil.getChildren(zkw, parentNode);
        if (nodeSet != null && nodeSet.isEmpty()) {
            ZKUtil.delZNode(this.zkw, parentNode);
        }
    }

    private String normalize(final String root) {
        if (root.startsWith("/")) {
            return this.removeLastSlash(root);
        } else {
            return "/" + this.removeLastSlash(root);
        }
    }

    private String removeLastSlash(final String root) {
        if (root.endsWith("/")) {
            return root.substring(0, root.lastIndexOf("/"));
        } else {
            return root;
        }
    }

}
