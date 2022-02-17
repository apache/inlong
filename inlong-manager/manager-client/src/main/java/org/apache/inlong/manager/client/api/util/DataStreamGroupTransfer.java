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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.DataStreamGroupConf;
import org.apache.inlong.manager.client.api.FlinkSortBaseConf;
import org.apache.inlong.manager.client.api.MqBaseConf;
import org.apache.inlong.manager.client.api.MqBaseConf.MqType;
import org.apache.inlong.manager.client.api.PulsarBaseConf;
import org.apache.inlong.manager.client.api.SortBaseConf;
import org.apache.inlong.manager.client.api.SortBaseConf.SortType;
import org.apache.inlong.manager.client.api.auth.Authentication;
import org.apache.inlong.manager.client.api.auth.Authentication.AuthType;
import org.apache.inlong.manager.client.api.auth.SecretTokenAuthentication;
import org.apache.inlong.manager.client.api.auth.TokenAuthentication;
import org.apache.inlong.manager.common.pojo.business.BusinessExtInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.common.settings.BusinessSettings;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.shiro.util.Assert;

public class DataStreamGroupTransfer {

    public static BusinessInfo createBusinessInfo(DataStreamGroupConf groupConf) {
        BusinessInfo businessInfo = new BusinessInfo();
        Assert.hasLength(groupConf.getGroupName(), "StreamGroupName should not be empty");
        businessInfo.setName(groupConf.getGroupName());
        businessInfo.setCnName(groupConf.getCnName());
        businessInfo.setDescription(groupConf.getDescription());
        businessInfo.setDailyRecords(groupConf.getDailyRecords());
        businessInfo.setPeakRecords(groupConf.getPeakRecords());
        businessInfo.setMaxLength(groupConf.getMaxLength());
        MqBaseConf mqConf = groupConf.getMqBaseConf();
        MqType mqType = mqConf.getType();
        businessInfo.setMiddlewareType(mqType.name());
        businessInfo.setExtList(Lists.newArrayList());
        businessInfo.setCreator(groupConf.getOperator());
        if (mqType == MqType.PULSAR) {
            PulsarBaseConf pulsarBaseConf = (PulsarBaseConf) mqConf;
            businessInfo.setMqResourceObj(pulsarBaseConf.getNamespace());
            BusinessPulsarInfo pulsarInfo = createPulsarInfo(pulsarBaseConf);
            businessInfo.setMqExtInfo(pulsarInfo);
            List<BusinessExtInfo> extInfos = createPulsarExtInfo(pulsarBaseConf);
            businessInfo.getExtList().addAll(extInfos);
            businessInfo.setTopicPartitionNum(pulsarBaseConf.getTopicPartitionNum());
        } else {
            // todo tubemq
        }
        SortBaseConf sortBaseConf = groupConf.getSortBaseConf();
        SortType sortType = sortBaseConf.getType();
        if (sortType == SortType.FLINK) {
            FlinkSortBaseConf flinkSortBaseConf = (FlinkSortBaseConf) sortBaseConf;
            List<BusinessExtInfo> sortExtInfos = createFlinkExtInfo(flinkSortBaseConf);
            businessInfo.getExtList().addAll(sortExtInfos);
        } else {
            //todo local
        }
        return businessInfo;
    }

    public static BusinessPulsarInfo createPulsarInfo(PulsarBaseConf pulsarBaseConf) {
        BusinessPulsarInfo pulsarInfo = new BusinessPulsarInfo();
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

    public static List<BusinessExtInfo> createPulsarExtInfo(PulsarBaseConf pulsarBaseConf) {
        List<BusinessExtInfo> extInfos = new ArrayList<>();
        if (pulsarBaseConf.getAuthentication() != null) {
            Authentication authentication = pulsarBaseConf.getAuthentication();
            AuthType authType = authentication.getAuthType();
            Assert.isTrue(authType == AuthType.TOKEN,
                    String.format("Unsupported authentication:%s for pulsar", authType.name()));
            TokenAuthentication tokenAuthentication = (TokenAuthentication) authentication;
            BusinessExtInfo authTypeExt = new BusinessExtInfo();
            authTypeExt.setKeyName(BusinessSettings.PULSAR_AUTHENTICATION_TYPE);
            authTypeExt.setKeyValue(tokenAuthentication.getAuthType().toString());
            extInfos.add(authTypeExt);
            BusinessExtInfo authValue = new BusinessExtInfo();
            authValue.setKeyName(BusinessSettings.PULSAR_AUTHENTICATION);
            authValue.setKeyValue(tokenAuthentication.getToken());
            extInfos.add(authValue);
        }
        if (StringUtils.isNotEmpty(pulsarBaseConf.getPulsarAdminUrl())) {
            BusinessExtInfo pulsarAdminUrl = new BusinessExtInfo();
            pulsarAdminUrl.setKeyName(BusinessSettings.PULSAR_ADMIN_URL);
            pulsarAdminUrl.setKeyValue(pulsarBaseConf.getPulsarAdminUrl());
            extInfos.add(pulsarAdminUrl);
        }
        if (StringUtils.isNotEmpty(pulsarBaseConf.getPulsarServiceUrl())) {
            BusinessExtInfo pulsarServiceUrl = new BusinessExtInfo();
            pulsarServiceUrl.setKeyName(BusinessSettings.PULSAR_SERVICE_URL);
            pulsarServiceUrl.setKeyValue(pulsarBaseConf.getPulsarServiceUrl());
            extInfos.add(pulsarServiceUrl);
        }
        return extInfos;
    }

    public static List<BusinessExtInfo> createFlinkExtInfo(FlinkSortBaseConf flinkSortBaseConf) {
        List<BusinessExtInfo> extInfos = new ArrayList<>();
        if (flinkSortBaseConf.getAuthentication() != null) {
            Authentication authentication = flinkSortBaseConf.getAuthentication();
            AuthType authType = authentication.getAuthType();
            Assert.isTrue(authType == AuthType.SECRET_AND_TOKEN,
                    String.format("Unsupported authentication:%s for flink", authType.name()));
            final SecretTokenAuthentication secretTokenAuthentication = (SecretTokenAuthentication) authentication;
            BusinessExtInfo authTypeExt = new BusinessExtInfo();
            authTypeExt.setKeyName(BusinessSettings.SORT_AUTHENTICATION_TYPE);
            authTypeExt.setKeyValue(authType.toString());
            extInfos.add(authTypeExt);
            BusinessExtInfo authValue = new BusinessExtInfo();
            authValue.setKeyName(BusinessSettings.SORT_AUTHENTICATION);
            authValue.setKeyValue(secretTokenAuthentication.toString());
            extInfos.add(authValue);
        }
        if (StringUtils.isNotEmpty(flinkSortBaseConf.getServiceUrl())) {
            BusinessExtInfo flinkUrl = new BusinessExtInfo();
            flinkUrl.setKeyName(BusinessSettings.SORT_URL);
            flinkUrl.setKeyValue(flinkSortBaseConf.getServiceUrl());
            extInfos.add(flinkUrl);
        }
        if (MapUtils.isNotEmpty(flinkSortBaseConf.getProperties())) {
            BusinessExtInfo flinkProperties = new BusinessExtInfo();
            flinkProperties.setKeyValue(BusinessSettings.SORT_PROPERTIES);
            flinkProperties.setKeyValue(JsonUtils.toJson(flinkSortBaseConf.getProperties()));
            extInfos.add(flinkProperties);
        }
        return extInfos;
    }
}
