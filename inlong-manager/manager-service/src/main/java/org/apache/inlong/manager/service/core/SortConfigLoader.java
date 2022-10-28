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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamExtEntity;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceClusterInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceGroupInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceStreamInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceStreamSinkInfo;

import java.util.List;

/**
 * Loader for sort service to load configs thought Cursor
 */
public interface SortConfigLoader {
    /**
     * Load all clusters by cursor
     * @return List of clusters, including MQ cluster and DataNode cluster.
     */
    List<SortSourceClusterInfo> loadAllClusters();

    /**
     * Load stream sinks by cursor
     * @return List of Stream sinks.
     */
    List<SortSourceStreamSinkInfo> loadAllStreamSinks();

    /**
     * Load groups by cursor
     * @return List of group info
     */
    List<SortSourceGroupInfo> loadAllGroup();

    /**
     * Load group backup info by cursor
     * @param keyName Key name
     * @return List of group backup info
     */
    List<InlongGroupExtEntity> loadGroupBackupInfo(String keyName);

    /**
     * Load stream backup info by cursor
     * @param keyName Key name
     * @return List of stream backup info
     */
    List<InlongStreamExtEntity> loadStreamBackupInfo(String keyName);

    /**
     * Load all inlong stream info by cursor
     * @return List of stream info
     */
    List<SortSourceStreamInfo> loadAllStreams();
}
