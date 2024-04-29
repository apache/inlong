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

package org.apache.inlong.dataproxy.config.holder;

import org.apache.inlong.common.heartbeat.AddressInfo;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * Source report configure holder
 *
 */
public class SourceReportConfigHolder {

    public static final Logger LOG =
            LoggerFactory.getLogger(SourceReportConfigHolder.class);

    private final Map<String, AddressInfo> srcAddressMap = new ConcurrentHashMap<>();

    public SourceReportConfigHolder() {

    }

    public void addSourceInfo(String sourceIp,
            String sourcePort, String rptSrcType, String protocolType) {
        if (StringUtils.isEmpty(sourceIp)
                || StringUtils.isEmpty(sourcePort)
                || StringUtils.isEmpty(rptSrcType)
                || StringUtils.isEmpty(protocolType)) {
            LOG.warn("[Source Report Holder] found empty parameter!, add values is {}, {}, {}, {}",
                    sourceIp, sourcePort, rptSrcType, protocolType);
            return;
        }
        String recordKey = sourceIp + "#" + sourcePort + "#" + protocolType;
        this.srcAddressMap.put(recordKey,
                new AddressInfo(sourceIp, sourcePort, rptSrcType, protocolType));
    }

    public Map<String, AddressInfo> getSrcAddressInfos() {
        return this.srcAddressMap;
    }

}
