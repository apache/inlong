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

import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.InlongGroupNoneConf;
import org.apache.inlong.manager.client.api.InlongGroupPulsarConf;
import org.apache.inlong.manager.client.api.InlongGroupTubeConf;
import org.apache.inlong.manager.common.auth.Authentication;
import org.apache.inlong.manager.common.auth.Authentication.AuthType;
import org.apache.inlong.manager.common.auth.SecretTokenAuthentication;
import org.apache.inlong.manager.common.auth.TokenAuthentication;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.tube.InlongTubeInfo;
import org.apache.inlong.manager.common.pojo.sort.BaseSortConf;
import org.apache.inlong.manager.common.pojo.sort.BaseSortConf.SortType;
import org.apache.inlong.manager.common.pojo.sort.FlinkSortConf;
import org.apache.inlong.manager.common.pojo.sort.UserDefinedSortConf;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.common.util.AssertUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The transfer util for Inlong Group
 */
public class InlongGroupTransfer {

    /**
     * Parse MQ config from inlong group info.
     */
    public static InlongGroupConf parseMQConf(InlongGroupInfo groupInfo) {
        if (null == groupInfo) {
            return null;
        }

        MQType mqType = MQType.forType(groupInfo.getMqType());
        switch (mqType) {
            case PULSAR:
            case TDMQ_PULSAR:
                return parsePulsarConf(groupInfo);
            case TUBE:
                return parseTubeConf(groupInfo);
            case NONE:
                return new InlongGroupNoneConf();
            default:
                throw new RuntimeException(String.format("Illegal MQ type=%s for Inlong", mqType));
        }
    }

    /**
     * Parse sort config from the inlong group.
     */
    public static BaseSortConf parseSortConf(InlongGroupInfo groupInfo) {
        List<InlongGroupExtInfo> groupExtInfos = groupInfo.getExtList();
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
                throw new IllegalArgumentException(String.format("Unsupported sort type=%s for Inlong", sortType));
        }
    }

    /**
     * Parse sort config of Flink.
     */
    private static FlinkSortConf parseFlinkSortConf(List<InlongGroupExtInfo> groupExtInfos) {
        FlinkSortConf flinkSortConf = new FlinkSortConf();
        for (InlongGroupExtInfo extInfo : groupExtInfos) {
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_URL)) {
                flinkSortConf.setServiceUrl(extInfo.getKeyValue());
            }
            if (extInfo.getKeyName().equals(InlongGroupSettings.SORT_PROPERTIES)) {
                Map<String, String> properties = GsonUtil.fromJson(extInfo.getKeyValue(),
                        new TypeToken<Map<String, String>>() {
                        }.getType());
                flinkSortConf.setProperties(properties);
            }
        }
        return flinkSortConf;
    }

    /**
     * Parse sort config of UDF.
     */
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

    /**
     * Parse Pulsar config from inlong group info.
     */
    private static InlongGroupPulsarConf parsePulsarConf(InlongGroupInfo groupInfo) {
        InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
        InlongGroupPulsarConf pulsarConf = new InlongGroupPulsarConf();
        pulsarConf.setTenant(pulsarInfo.getTenant());
        pulsarConf.setAdminUrl(pulsarInfo.getAdminUrl());
        pulsarConf.setServiceUrl(pulsarInfo.getServiceUrl());
        pulsarConf.setQueueModule(pulsarInfo.getQueueModule());
        pulsarConf.setPartitionNum(pulsarInfo.getPartitionNum());
        pulsarConf.setEnsemble(pulsarInfo.getEnsemble());
        pulsarConf.setWriteQuorum(pulsarInfo.getWriteQuorum());
        pulsarConf.setAckQuorum(pulsarInfo.getAckQuorum());
        pulsarConf.setTtl(pulsarInfo.getTtl());
        pulsarConf.setTtlUnit(pulsarInfo.getTtlUnit());
        pulsarConf.setRetentionTime(pulsarInfo.getRetentionTime());
        pulsarConf.setRetentionTimeUnit(pulsarInfo.getRetentionTimeUnit());
        pulsarConf.setRetentionSize(pulsarInfo.getRetentionSize());
        pulsarConf.setRetentionSizeUnit(pulsarInfo.getRetentionSizeUnit());

        return pulsarConf;
    }

    /**
     * Parse Tube config from inlong group info.
     */
    private static InlongGroupTubeConf parseTubeConf(InlongGroupInfo groupInfo) {
        InlongTubeInfo tubeInfo = (InlongTubeInfo) groupInfo;
        InlongGroupTubeConf tubeConf = new InlongGroupTubeConf();
        tubeConf.setManagerUrl(tubeInfo.getManagerUrl());
        tubeConf.setMasterUrl(tubeInfo.getMasterUrl());
        tubeConf.setClusterId(tubeInfo.getClusterId());

        return tubeConf;
    }

    /**
     * Create inlong group info from group config.
     */
    public static InlongGroupInfo createGroupInfo(InlongGroupInfo originGroupInfo, BaseSortConf sortConf) {
        AssertUtils.notNull(originGroupInfo, "Inlong group info cannot be null");
        AssertUtils.hasLength(originGroupInfo.getInlongGroupId(), "groupId cannot be empty");

        // set authentication into group ext list
        List<InlongGroupExtInfo> extInfos = new ArrayList<>();
        if (originGroupInfo.getAuthentication() != null) {
            Authentication authentication = originGroupInfo.getAuthentication();
            AuthType authType = authentication.getAuthType();
            AssertUtils.isTrue(authType == AuthType.TOKEN,
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

            originGroupInfo.getExtList().addAll(extInfos);
        }

        // set the sort config into ext list
        SortType sortType = sortConf.getType();
        List<InlongGroupExtInfo> sortExtInfos;
        if (sortType == SortType.FLINK) {
            FlinkSortConf flinkSortConf = (FlinkSortConf) sortConf;
            sortExtInfos = createFlinkExtInfo(flinkSortConf);
        } else if (sortType == SortType.USER_DEFINED) {
            UserDefinedSortConf udf = (UserDefinedSortConf) sortConf;
            sortExtInfos = createUserDefinedSortExtInfo(udf);
        } else {
            // todo local
            sortExtInfos = new ArrayList<>();
        }

        originGroupInfo.getExtList().addAll(sortExtInfos);
        return originGroupInfo;
    }

    /**
     * Get ext infos from flink config
     */
    public static List<InlongGroupExtInfo> createFlinkExtInfo(FlinkSortConf flinkSortConf) {
        List<InlongGroupExtInfo> extInfos = new ArrayList<>();
        InlongGroupExtInfo sortType = new InlongGroupExtInfo();
        sortType.setKeyName(InlongGroupSettings.SORT_TYPE);
        sortType.setKeyValue(SortType.FLINK.getType());
        extInfos.add(sortType);
        if (flinkSortConf.getAuthentication() != null) {
            Authentication authentication = flinkSortConf.getAuthentication();
            AuthType authType = authentication.getAuthType();
            AssertUtils.isTrue(authType == AuthType.SECRET_AND_TOKEN,
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
        if (StringUtils.isNotEmpty(flinkSortConf.getServiceUrl())) {
            InlongGroupExtInfo flinkUrl = new InlongGroupExtInfo();
            flinkUrl.setKeyName(InlongGroupSettings.SORT_URL);
            flinkUrl.setKeyValue(flinkSortConf.getServiceUrl());
            extInfos.add(flinkUrl);
        }
        if (MapUtils.isNotEmpty(flinkSortConf.getProperties())) {
            InlongGroupExtInfo flinkProperties = new InlongGroupExtInfo();
            flinkProperties.setKeyName(InlongGroupSettings.SORT_PROPERTIES);
            flinkProperties.setKeyValue(JsonUtils.toJson(flinkSortConf.getProperties()));
            extInfos.add(flinkProperties);
        }
        return extInfos;
    }

    /**
     * Get ext infos from user defined sort config
     */
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
