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

package org.apache.tubemq.server.master.nodemanage.nodebroker;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tubemq.corebase.cluster.Partition;
import org.apache.tubemq.corebase.cluster.TopicInfo;
import org.apache.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.common.statusdef.ManageStatus;


public class BrokerPSInfoHolder {
    // broker manage status
    private ConcurrentHashSet<Integer/* brokerId */> enablePubBrokerIdSet = new ConcurrentHashSet<>();
    private ConcurrentHashSet<Integer/* brokerId */> enableSubBrokerIdSet = new ConcurrentHashSet<>();
    // broker subscribe topic view info
    private BrokerTopicInfoView subTopicInfoView = new BrokerTopicInfoView();
    // broker publish topic view info
    private BrokerTopicInfoView pubTopicInfoView = new BrokerTopicInfoView();


    public BrokerPSInfoHolder() {

    }

    /**
     * remove broker all configure info
     *
     * @param brokerId broker id index
     * @param isTimeout if broker is timeout
     */
    public void rmvBrokerAllPushedInfo(int brokerId, boolean isTimeout) {
        // remove broker status Info
        enablePubBrokerIdSet.remove(brokerId);
        enableSubBrokerIdSet.remove(brokerId);
        if (!isTimeout) {
            // remove broker topic info
            subTopicInfoView.rmvBrokerTopicInfo(brokerId);
            pubTopicInfoView.rmvBrokerTopicInfo(brokerId);
        }
    }

    /**
     * update broker manage status
     *
     * @param brokerId broker id index
     * @param mngStatus broker's manage status
     */
    public void updBrokerMangeStatus(int brokerId, ManageStatus mngStatus) {
        Tuple2<Boolean, Boolean> pubSubStatus = mngStatus.getPubSubStatus();
        if (pubSubStatus.getF0() == Boolean.TRUE) {
            enablePubBrokerIdSet.add(brokerId);
        } else {
            enablePubBrokerIdSet.remove(brokerId);
        }
        if (pubSubStatus.getF1() == Boolean.TRUE) {
            enableSubBrokerIdSet.add(brokerId);
        } else {
            enableSubBrokerIdSet.remove(brokerId);
        }
    }

    public Tuple2<Boolean, Boolean> getBrokerPubStatus(int brokerId) {
        return new Tuple2<>(enablePubBrokerIdSet.contains(brokerId),
                enableSubBrokerIdSet.contains(brokerId));
    }

    /**
     * update broker's subscribe topicInfo configures
     *
     * @param brokerId broker id index
     * @param topicInfoMap broker's topic configure info,
     *                    if topicInfoMap is null, reserve current configure;
     *                    if topicInfoMap is empty, clear current configure.
     * @return if fast sync data
     */
    public boolean updBrokerSubTopicConfInfo(int brokerId,
                                          Map<String, TopicInfo> topicInfoMap) {
        if (topicInfoMap == null) {
            return true;
        }
        subTopicInfoView.updBrokerTopicConfInfo(brokerId, topicInfoMap);
        return pubTopicInfoView.fastUpdBrokerTopicConfInfo(brokerId, topicInfoMap);
    }

    /**
     * update broker's publish topicInfo configures
     *
     * @param brokerId broker id index
     * @param topicInfoMap broker's topic configure info,
     *                    if topicInfoMap is null, reserve current configure;
     *                    if topicInfoMap is empty, clear current configure.
     */
    public void updBrokerPubTopicConfInfo(int brokerId,
                                          Map<String, TopicInfo> topicInfoMap) {
        if (topicInfoMap == null) {
            return;
        }
        pubTopicInfoView.updBrokerTopicConfInfo(brokerId, topicInfoMap);
    }

    /**
     * Get the maximum number of broker distributions of topic
     *
     * @param topicSet need query topic set
     */
    public int getTopicMaxSubBrokerCnt(Set<String> topicSet) {
        return subTopicInfoView.getMaxTopicBrokerCnt(topicSet);
    }

    /**
     * Gets the map of topic partitions whose subscribe status is enabled
     *
     * @param topicSet need query topic set
     */
    public Map<String, Partition> getAcceptSubParts(Set<String> topicSet) {
        return subTopicInfoView.getAcceptSubParts(topicSet, enableSubBrokerIdSet);
    }

    /**
     * Gets the list of topic partitions whose subscribe status is enabled
     *
     * @param topic need query topic set
     */
    public List<Partition> getAcceptSubParts(String topic) {
        return subTopicInfoView.getAcceptSubParts(topic, enableSubBrokerIdSet);
    }

    /**
     * Gets the string map of topic partitions whose publish status is enabled
     *
     * @param topicSet need query topic set
     */
    public Map<String, String> getAcceptPubPartInfo(Set<String> topicSet) {
        return pubTopicInfoView.getAcceptPubPartInfo(topicSet, enablePubBrokerIdSet);
    }

    /**
     * Get the published TopicInfo information of topic in broker
     *
     * @param brokerId need query broker
     * @param topic    need query topic
     *
     * @return null or topicInfo configure
     */
    public TopicInfo getBrokerPubPushedTopicInfo(int brokerId, String topic) {
        return pubTopicInfoView.getBrokerPushedTopicInfo(brokerId, topic);
    }

    /**
     * Get all published TopicInfo information of broker
     *
     * @param brokerId need query broker
     */
    public List<TopicInfo> getPubBrokerPushedTopicInfo(int brokerId) {
        return pubTopicInfoView.getBrokerPushedTopicInfo(brokerId);
    }

}
