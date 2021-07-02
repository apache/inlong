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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * bid to m value
 */
public class BidPropertiesHolder extends PropertiesConfigHolder {

    private static final Logger LOG = LoggerFactory.getLogger(BidPropertiesHolder.class);
    private static final String BID_VALUE_SPLITTER = "#";


    private Map<String, String> bidMappingProperties =
            new HashMap<String, String>();
    private Map<String, Map<String, String>> tidMappingProperties =
            new HashMap<String, Map<String, String>>();
    private Map<String, String> bidEnableMappingProperties =
            new HashMap<String, String>();

    public BidPropertiesHolder(String fileName) {
        super(fileName);
    }

    @Override
    public void loadFromFileToHolder() {
        super.loadFromFileToHolder();
        try {
            Map<String, String> tmpBidMappingProperties =
                    new HashMap<String, String>();
            Map<String, Map<String, String>> tmpTidMappingProperties =
                    new HashMap<String, Map<String, String>>();
            Map<String, String> tmpBidEnableMappingProperties = new HashMap<String, String>();
            for (Map.Entry<String, String> entry : super.getHolder().entrySet()) {
                String[] sArray = StringUtils.split(entry.getKey(), BID_VALUE_SPLITTER);
                if (sArray.length != 3) {
                    LOG.warn("invalid bid key {}", entry.getKey());
                    continue;
                }
                tmpBidMappingProperties.put(sArray[0].trim(), sArray[1].trim());
                tmpBidEnableMappingProperties.put(sArray[0].trim(), sArray[2].trim());
                if (StringUtils.isNotBlank(entry.getValue())) {
                    tmpTidMappingProperties.put(sArray[0].trim(),
                            MAP_SPLITTER.split(entry.getValue()));
                }
            }
            bidMappingProperties = tmpBidMappingProperties;
            tidMappingProperties = tmpTidMappingProperties;
            bidEnableMappingProperties = tmpBidEnableMappingProperties;
        } catch (Exception e) {
            LOG.error("loadConfig error :", e);
        }
    }

    public Map<String, String> getBidMappingProperties() {
        return bidMappingProperties;
    }

    public Map<String, Map<String, String>> getTidMappingProperties() {
        return tidMappingProperties;
    }

    public Map<String, String> getBidEnableMappingProperties() {
        return bidEnableMappingProperties;
    }
}
