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

package org.apache.inlong.manager.service.core.impl;

import org.apache.ibatis.cursor.Cursor;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamExtEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamExtEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceClusterInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceGroupInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceStreamInfo;
import org.apache.inlong.manager.pojo.sort.standalone.SortSourceStreamSinkInfo;
import org.apache.inlong.manager.service.core.SortConfigLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
public class SortConfigLoaderImpl implements SortConfigLoader {
    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;
    @Autowired
    private StreamSinkEntityMapper streamSinkEntityMapper;
    @Autowired
    private InlongGroupEntityMapper inlongGroupEntityMapper;
    @Autowired
    private InlongGroupExtEntityMapper inlongGroupExtEntityMapper;
    @Autowired
    private InlongStreamExtEntityMapper inlongStreamExtEntityMapper;
    @Autowired
    private InlongStreamEntityMapper inlongStreamEntityMapper;

    @Transactional
    @Override
    public List<SortSourceClusterInfo> loadAllClusters() {
        Cursor<SortSourceClusterInfo> cursor = clusterEntityMapper.selectAllClusters();
        List<SortSourceClusterInfo> allClusters = new ArrayList<>();
        cursor.forEach(allClusters::add);
        return allClusters;
    }

    @Transactional
    @Override
    public List<SortSourceStreamSinkInfo> loadAllStreamSinks() {
        Cursor<SortSourceStreamSinkInfo> cursor = streamSinkEntityMapper.selectAllStreams();
        List<SortSourceStreamSinkInfo> allStreamSinks = new ArrayList<>();
        cursor.forEach(allStreamSinks::add);
        return allStreamSinks;
    }

    @Transactional
    @Override
    public List<SortSourceGroupInfo> loadAllGroup() {
        Cursor<SortSourceGroupInfo> cursor = inlongGroupEntityMapper.selectAllGroups();
        List<SortSourceGroupInfo> allGroups = new ArrayList<>();
        cursor.forEach(allGroups::add);
        return allGroups;
    }

    @Transactional
    @Override
    public List<InlongGroupExtEntity> loadGroupBackupInfo(String keyName) {
        Cursor<InlongGroupExtEntity> cursor = inlongGroupExtEntityMapper.selectByKeyName(keyName);
        List<InlongGroupExtEntity> groupBackupInfo = new ArrayList<>();
        cursor.forEach(groupBackupInfo::add);
        return groupBackupInfo;
    }

    @Transactional
    @Override
    public List<InlongStreamExtEntity> loadStreamBackupInfo(String keyName) {
        Cursor<InlongStreamExtEntity> cursor = inlongStreamExtEntityMapper.selectByKeyName(keyName);
        List<InlongStreamExtEntity> streamBackupInfo = new ArrayList<>();
        cursor.forEach(streamBackupInfo::add);
        return streamBackupInfo;
    }

    @Transactional
    @Override
    public List<SortSourceStreamInfo> loadAllStreams() {
        Cursor<SortSourceStreamInfo> cursor = inlongStreamEntityMapper.selectAllStreams();
        List<SortSourceStreamInfo> allStreams = new ArrayList<>();
        cursor.forEach(allStreams::add);
        return allStreams;
    }
}
