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

package org.apache.inlong.manager.client.api.util;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.FlinkSortBaseConf;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.MqBaseConf;
import org.apache.inlong.manager.client.api.MqBaseConf.MqType;
import org.apache.inlong.manager.client.api.PulsarBaseConf;
import org.apache.inlong.manager.client.api.SortBaseConf;
import org.apache.inlong.manager.client.api.SortBaseConf.SortType;
import org.apache.inlong.manager.client.api.TubeBaseConf;
import org.apache.inlong.manager.client.api.UserDefinedSortConf;
import org.apache.inlong.manager.client.api.auth.Authentication;
import org.apache.inlong.manager.client.api.auth.Authentication.AuthType;
import org.apache.inlong.manager.client.api.auth.SecretTokenAuthentication;
import org.apache.inlong.manager.client.api.auth.TokenAuthentication;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupMqExtBase;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.common.util.JsonUtils;

public class InlongGroupTransfer {

    public static InlongGroupConf parseGroupRequest(InlongGroupRequest groupRequest) {
        InlongGroupConf inlongGroupConf = new InlongGroupConf();
        inlongGroupConf.setGroupName(groupRequest.getName());
        inlongGroupConf.setDescription(groupRequest.getDescription());
        inlongGroupConf.setCnName(groupRequest.getCnName());
        inlongGroupConf.setZookeeperEnabled(groupRequest.getZookeeperEnabled() == 1);
        inlongGroupConf.setDailyRecords(Long.valueOf(groupRequest.getDailyRecords()));
        inlongGroupConf.setPeakRecords(Long.valueOf(groupRequest.getPeakRecords()));
        inlongGroupConf.setMqBaseConf(parseMqBaseConf(groupRequest));
        inlongGroupConf.setSortBaseConf(parseSortBaseConf(groupRequest));
        return inlongGroupConf;
    }

    public static MqBaseConf parseMqBaseConf(InlongGroupRequest inlongGroupRequest) {
        InlongGroupMqExtBase mqExtBase = inlongGroupRequest.getMqExtInfo();
        String middleWare = mqExtBase.getMiddlewareType();
        MqType mqType = MqType.forType(middleWare);
        switch (mqType) {
            case NONE:
                return MqBaseConf.BLANK_MQ_CONF;
            case PULSAR:
                return parsePulsarConf(inlongGroupRequest);
            case TUBE:
                return parseTubeConf(inlongGroupRequest);
            default:
                throw new RuntimeException(String.format("Illegal mqType=%s for Inlong", mqType));
        }
    }

    public static SortBaseConf parseSortBaseConf(InlongGroupRequest groupRequest) {
        List<InlongGroupExtInfo> groupExtInfos = groupRequest.getExtList();
        if (CollectionUtils.isEmpty(groupExtInfos)) {
            return null;
        }
        String type = null;
        for (InlongGroupExtInfo extInfo : groupExtInfos) {
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_TYPE)) {
                type = extInfo.getKeyValue();
                break;
            }
        }
        if (type == null) {
            return null;
        }
        SortType sortType = SortType.forType(type);
        switch (sortType) {
            case FLINK:
                return parseFlinkSortConf(groupExtInfos);
            case USER_DEFINED:
                return parseUdf(groupExtInfos);
            default:
                throw new IllegalArgumentException(String.format("Unsupport sort type=%s for Inlong", sortType));
        }
    }

    private static FlinkSortBaseConf parseFlinkSortConf(List<InlongGroupExtInfo> groupExtInfos) {
        FlinkSortBaseConf sortBaseConf = new FlinkSortBaseConf();
        for (InlongGroupExtInfo extInfo : groupExtInfos) {
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_URL)) {
                sortBaseConf.setServiceUrl(extInfo.getKeyValue());
            }
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_PROPERTIES)) {
                Map<String, String> properties = GsonUtil.fromJson(extInfo.getKeyValue(),
                        new TypeToken<Map<String, String>>() {
                        }.getType());
                sortBaseConf.setProperties(properties);
            }
        }
        return sortBaseConf;
    }

    private static UserDefinedSortConf parseUdf(List<InlongGroupExtInfo> groupExtInfos) {
        UserDefinedSortConf sortConf = new UserDefinedSortConf();
        for (InlongGroupExtInfo extInfo : groupExtInfos) {
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_NAME)) {
                sortConf.setSortName(extInfo.getKeyValue());
            }
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_PROPERTIES)) {
                Map<String, String> properties = GsonUtil.fromJson(extInfo.getKeyValue(),
                        new TypeToken<Map<String, String>>() {
                        }.getType());
                sortConf.setProperties(properties);
            }
        }
        return sortConf;
    }

    private static PulsarBaseConf parsePulsarConf(InlongGroupRequest groupRequest) {
        PulsarBaseConf pulsarBaseConf = new PulsarBaseConf();
        pulsarBaseConf.setNamespace(groupRequest.getMqResourceObj());
        InlongGroupPulsarInfo inlongGroupPulsarInfo = (InlongGroupPulsarInfo) groupRequest.getMqExtInfo();
        pulsarBaseConf.setAckQuorum(inlongGroupPulsarInfo.getAckQuorum());
        pulsarBaseConf.setWriteQuorum(inlongGroupPulsarInfo.getWriteQuorum());
        pulsarBaseConf.setEnsemble(inlongGroupPulsarInfo.getEnsemble());
        pulsarBaseConf.setTtl(inlongGroupPulsarInfo.getTtl());
        pulsarBaseConf.setRetentionTime(inlongGroupPulsarInfo.getRetentionTime());
        pulsarBaseConf.setRetentionSize(inlongGroupPulsarInfo.getRetentionSize());
        pulsarBaseConf.setRetentionSizeUnit(inlongGroupPulsarInfo.getRetentionSizeUnit());
        pulsarBaseConf.setRetentionTimeUnit(inlongGroupPulsarInfo.getRetentionTimeUnit());
        List<InlongGroupExtInfo> groupExtInfos = groupRequest.getExtList();
        for (InlongGroupExtInfo extInfo : groupExtInfos) {
            if (extInfo.getKeyName().equals(InlongGroupSettings.PULSAR_ADMIN_URL)) {
                pulsarBaseConf.setPulsarAdminUrl(extInfo.getKeyValue());
            }
            if (extInfo.getKeyName().equals(InlongGroupSettings.PULSAR_SERVICE_URL)) {
                pulsarBaseConf.setPulsarServiceUrl(extInfo.getKeyValue());
            }
        }
        return pulsarBaseConf;
    }

    private static TubeBaseConf parseTubeConf(InlongGroupRequest groupRequest) {
        TubeBaseConf tubeBaseConf = new TubeBaseConf();
        tubeBaseConf.setGroupName(groupRequest.getMqResourceObj());
        List<InlongGroupExtInfo> groupExtInfos = groupRequest.getExtList();
        for (InlongGroupExtInfo extInfo : groupExtInfos) {
            if (extInfo.getKeyName().equals(InlongGroupSettings.TUBE_CLUSTER_ID)) {
                tubeBaseConf.setTubeClusterId(Integer.parseInt(extInfo.getKeyValue()));
            }
            if (extInfo.getKeyName().equals(InlongGroupSettings.TUBE_MANAGER_URL)) {
                tubeBaseConf.setTubeManagerUrl(extInfo.getKeyValue());
            }
            if (extInfo.getKeyName().equals(InlongGroupSettings.TUBE_MASTER_URL)) {
                tubeBaseConf.setTubeMasterUrl(extInfo.getKeyValue());
            }
        }
        return tubeBaseConf;
    }

    public static InlongGroupRequest createGroupInfo(InlongGroupConf groupConf) {
        InlongGroupRequest groupInfo = new InlongGroupRequest();
        AssertUtil.hasLength(groupConf.getGroupName(), "GroupName should not be empty");
        groupInfo.setName(groupConf.getGroupName());
        groupInfo.setCnName(groupConf.getCnName());
        groupInfo.setDescription(groupConf.getDescription());
        groupInfo.setZookeeperEnabled(groupConf.isZookeeperEnabled() ? 1 : 0);
        groupInfo.setDailyRecords(groupConf.getDailyRecords().intValue());
        groupInfo.setPeakRecords(groupConf.getPeakRecords().intValue());
        groupInfo.setMaxLength(groupConf.getMaxLength());
        MqBaseConf mqConf = groupConf.getMqBaseConf();
        MqType mqType = mqConf.getType();
        groupInfo.setMiddlewareType(mqType.name());
        groupInfo.setInCharges(groupConf.getOperator());
        groupInfo.setExtList(Lists.newArrayList());
        groupInfo.setCreator(groupConf.getOperator());
        if (mqType == MqType.PULSAR) {
            PulsarBaseConf pulsarBaseConf = (PulsarBaseConf) mqConf;
            groupInfo.setMqResourceObj(pulsarBaseConf.getNamespace());
            InlongGroupPulsarInfo pulsarInfo = createPulsarInfo(pulsarBaseConf);
            groupInfo.setMqExtInfo(pulsarInfo);
            List<InlongGroupExtInfo> extInfos = createPulsarExtInfo(pulsarBaseConf);
            groupInfo.getExtList().addAll(extInfos);
            groupInfo.setTopicPartitionNum(pulsarBaseConf.getTopicPartitionNum());
        } else if (mqType == MqType.TUBE) {
            TubeBaseConf tubeBaseConf = (TubeBaseConf) mqConf;
            List<InlongGroupExtInfo> extInfos = createTubeExtInfo(tubeBaseConf);
            groupInfo.setMqResourceObj(tubeBaseConf.getGroupName());
            groupInfo.getExtList().addAll(extInfos);
            groupInfo.setTopicPartitionNum(tubeBaseConf.getTopicPartitionNum());
        }
        SortBaseConf sortBaseConf = groupConf.getSortBaseConf();
        SortType sortType = sortBaseConf.getType();
        if (sortType == SortType.FLINK) {
            FlinkSortBaseConf flinkSortBaseConf = (FlinkSortBaseConf) sortBaseConf;
            List<InlongGroupExtInfo> sortExtInfos = createFlinkExtInfo(flinkSortBaseConf);
            groupInfo.getExtList().addAll(sortExtInfos);
        } else if (sortType == SortType.USER_DEFINED) {
            UserDefinedSortConf udf = (UserDefinedSortConf) sortBaseConf;
            List<InlongGroupExtInfo> sortExtInfos = createUserDefinedSortExtInfo(udf);
            groupInfo.getExtList().addAll(sortExtInfos);
        } else {
            //todo local
        }
        return groupInfo;
    }

    public static InlongGroupPulsarInfo createPulsarInfo(PulsarBaseConf pulsarBaseConf) {
        InlongGroupPulsarInfo pulsarInfo = new InlongGroupPulsarInfo();
        pulsarInfo.setMiddlewareType(pulsarBaseConf.getType().name());
        pulsarInfo.setEnsemble(pulsarBaseConf.getEnsemble());
        pulsarInfo.setAckQuorum(pulsarBaseConf.getAckQuorum());
        pulsarInfo.setWriteQuorum(pulsarBaseConf.getWriteQuorum());
        pulsarInfo.setRetentionSize(pulsarBaseConf.getRetentionSize());
        pulsarInfo.setRetentionTime(pulsarBaseConf.getRetentionTime());
        pulsarInfo.setRetentionSizeUnit(pulsarBaseConf.getRetentionSizeUnit());
        pulsarInfo.setRetentionTimeUnit(pulsarBaseConf.getRetentionTimeUnit());
        pulsarInfo.setTtl(pulsarBaseConf.getTtl());
        pulsarInfo.setTtlUnit(pulsarBaseConf.getTtlUnit());
        return pulsarInfo;
    }

    public static List<InlongGroupExtInfo> createPulsarExtInfo(PulsarBaseConf pulsarBaseConf) {
        List<InlongGroupExtInfo> extInfos = new ArrayList<>();
        if (pulsarBaseConf.getAuthentication() != null) {
            Authentication authentication = pulsarBaseConf.getAuthentication();
            AuthType authType = authentication.getAuthType();
            AssertUtil.isTrue(authType == AuthType.TOKEN,
                    String.format("Unsupported authentication:%s for pulsar", authType.name()));
            TokenAuthentication tokenAuthentication = (TokenAuthentication) authentication;
            InlongGroupExtInfo authTypeExt = new InlongGroupExtInfo();
            authTypeExt.setKeyName(InlongGroupSettings.PULSAR_AUTHENTICATION_TYPE);
            authTypeExt.setKeyValue(tokenAuthentication.getAuthType().toString());
            extInfos.add(authTypeExt);
            InlongGroupExtInfo authValue = new InlongGroupExtInfo();
            authValue.setKeyName(InlongGroupSettings.PULSAR_AUTHENTICATION);
            authValue.setKeyValue(tokenAuthentication.getToken());
            extInfos.add(authValue);
        }
        if (StringUtils.isNotEmpty(pulsarBaseConf.getPulsarAdminUrl())) {
            InlongGroupExtInfo pulsarAdminUrl = new InlongGroupExtInfo();
            pulsarAdminUrl.setKeyName(InlongGroupSettings.PULSAR_ADMIN_URL);
            pulsarAdminUrl.setKeyValue(pulsarBaseConf.getPulsarAdminUrl());
            extInfos.add(pulsarAdminUrl);
        }
        if (StringUtils.isNotEmpty(pulsarBaseConf.getPulsarServiceUrl())) {
            InlongGroupExtInfo pulsarServiceUrl = new InlongGroupExtInfo();
            pulsarServiceUrl.setKeyName(InlongGroupSettings.PULSAR_SERVICE_URL);
            pulsarServiceUrl.setKeyValue(pulsarBaseConf.getPulsarServiceUrl());
            extInfos.add(pulsarServiceUrl);
        }
        return extInfos;
    }

    public static List<InlongGroupExtInfo> createTubeExtInfo(TubeBaseConf tubeBaseConf) {
        List<InlongGroupExtInfo> extInfos = new ArrayList<>();
        if (StringUtils.isNotEmpty(tubeBaseConf.getTubeMasterUrl())) {
            InlongGroupExtInfo tubeManagerUrl = new InlongGroupExtInfo();
            tubeManagerUrl.setKeyName(InlongGroupSettings.TUBE_MANAGER_URL);
            tubeManagerUrl.setKeyValue(tubeBaseConf.getTubeManagerUrl());
            extInfos.add(tubeManagerUrl);
        }
        if (StringUtils.isNotEmpty(tubeBaseConf.getTubeMasterUrl())) {
            InlongGroupExtInfo tubeMasterUrl = new InlongGroupExtInfo();
            tubeMasterUrl.setKeyName(InlongGroupSettings.TUBE_MASTER_URL);
            tubeMasterUrl.setKeyValue(tubeBaseConf.getTubeMasterUrl());
            extInfos.add(tubeMasterUrl);
        }
        if (tubeBaseConf.getTubeClusterId() > 0) {
            InlongGroupExtInfo tubeClusterId = new InlongGroupExtInfo();
            tubeClusterId.setKeyName(InlongGroupSettings.TUBE_CLUSTER_ID);
            tubeClusterId.setKeyValue(String.valueOf(tubeBaseConf.getTubeClusterId()));
            extInfos.add(tubeClusterId);
        }
        return extInfos;
    }

    public static List<InlongGroupExtInfo> createFlinkExtInfo(FlinkSortBaseConf flinkSortBaseConf) {
        List<InlongGroupExtInfo> extInfos = new ArrayList<>();
        InlongGroupExtInfo sortType = new InlongGroupExtInfo();
        sortType.setKeyName(InlongGroupSettings.SORT_TYPE);
        sortType.setKeyValue(SortType.FLINK.getType());
        extInfos.add(sortType);
        if (flinkSortBaseConf.getAuthentication() != null) {
            Authentication authentication = flinkSortBaseConf.getAuthentication();
            AuthType authType = authentication.getAuthType();
            AssertUtil.isTrue(authType == AuthType.SECRET_AND_TOKEN,
                    String.format("Unsupported authentication:%s for flink", authType.name()));
            final SecretTokenAuthentication secretTokenAuthentication = (SecretTokenAuthentication) authentication;
            InlongGroupExtInfo authTypeExt = new InlongGroupExtInfo();
            authTypeExt.setKeyName(InlongGroupSettings.SORT_AUTHENTICATION_TYPE);
            authTypeExt.setKeyValue(authType.toString());
            extInfos.add(authTypeExt);
            InlongGroupExtInfo authValue = new InlongGroupExtInfo();
            authValue.setKeyName(InlongGroupSettings.SORT_AUTHENTICATION);
            authValue.setKeyValue(secretTokenAuthentication.toString());
            extInfos.add(authValue);
        }
        if (StringUtils.isNotEmpty(flinkSortBaseConf.getServiceUrl())) {
            InlongGroupExtInfo flinkUrl = new InlongGroupExtInfo();
            flinkUrl.setKeyName(InlongGroupSettings.SORT_URL);
            flinkUrl.setKeyValue(flinkSortBaseConf.getServiceUrl());
            extInfos.add(flinkUrl);
        }
        if (MapUtils.isNotEmpty(flinkSortBaseConf.getProperties())) {
            InlongGroupExtInfo flinkProperties = new InlongGroupExtInfo();
            flinkProperties.setKeyName(InlongGroupSettings.SORT_PROPERTIES);
            flinkProperties.setKeyValue(JsonUtils.toJson(flinkSortBaseConf.getProperties()));
            extInfos.add(flinkProperties);
        }
        return extInfos;
    }

    public static List<InlongGroupExtInfo> createUserDefinedSortExtInfo(UserDefinedSortConf userDefinedSortConf) {
        List<InlongGroupExtInfo> extInfos = new ArrayList<>();
        InlongGroupExtInfo sortType = new InlongGroupExtInfo();
        sortType.setKeyName(InlongGroupSettings.SORT_TYPE);
        sortType.setKeyValue(SortType.USER_DEFINED.getType());
        extInfos.add(sortType);
        InlongGroupExtInfo sortName = new InlongGroupExtInfo();
        sortName.setKeyName(InlongGroupSettings.SORT_NAME);
        sortName.setKeyValue(userDefinedSortConf.getSortName());
        extInfos.add(sortName);
        if (MapUtils.isNotEmpty(userDefinedSortConf.getProperties())) {
            InlongGroupExtInfo flinkProperties = new InlongGroupExtInfo();
            flinkProperties.setKeyName(InlongGroupSettings.SORT_PROPERTIES);
            flinkProperties.setKeyValue(JsonUtils.toJson(userDefinedSortConf.getProperties()));
            extInfos.add(flinkProperties);
        }
        return extInfos;
    }
}
