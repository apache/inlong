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

package org.apache.inlong.manager.dao.mapper;

import java.util.List;

import org.apache.inlong.manager.dao.entity.CacheCluster;
import org.apache.inlong.manager.dao.entity.CacheClusterExt;
import org.apache.inlong.manager.dao.entity.CacheTopic;
import org.apache.inlong.manager.dao.entity.ClusterSet;
import org.apache.inlong.manager.dao.entity.FlumeChannel;
import org.apache.inlong.manager.dao.entity.FlumeChannelExt;
import org.apache.inlong.manager.dao.entity.FlumeSink;
import org.apache.inlong.manager.dao.entity.FlumeSinkExt;
import org.apache.inlong.manager.dao.entity.FlumeSource;
import org.apache.inlong.manager.dao.entity.FlumeSourceExt;
import org.apache.inlong.manager.dao.entity.InLongId;
import org.apache.inlong.manager.dao.entity.ProxyCluster;
import org.apache.inlong.manager.dao.entity.ProxyClusterToCacheCluster;
import org.springframework.stereotype.Repository;

/**
 * ClusterSetMapper
 */
@Repository
public interface ClusterSetMapper {
    List<ClusterSet> selectClusterSet();

    List<InLongId> selectInlongId();

    List<CacheCluster> selectCacheCluster();

    List<CacheClusterExt> selectCacheClusterExt();

    List<CacheTopic> selectCacheTopic();

    List<ProxyCluster> selectProxyCluster();

    List<ProxyClusterToCacheCluster> selectProxyClusterToCacheCluster();

    List<FlumeSource> selectFlumeSource();

    List<FlumeSourceExt> selectFlumeSourceExt();

    List<FlumeChannel> selectFlumeChannel();

    List<FlumeChannelExt> selectFlumeChannelExt();

    List<FlumeSink> selectFlumeSink();

    List<FlumeSinkExt> selectFlumeSinkExt();

}
