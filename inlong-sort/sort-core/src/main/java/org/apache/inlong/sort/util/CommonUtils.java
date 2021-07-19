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

package org.apache.inlong.sort.util;

import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common utils.
 */
public class CommonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

    /**
     * Parse hostAndPort String to hostAndPort pair.
     * @param address String like "127.0.0.1:8080,127.0.0.2:8081"
     */
    public static List<Pair<String, Integer>> parseHostnameAndPort(String address) {
        final List<Pair<String, Integer>> addresses = new ArrayList<>();
        for (String masterAddress : address.split(",")) {
            final String[] addressItems = masterAddress.split(":");
            if (addressItems.length != 2) {
                LOG.warn("No host or port of address {}, ignore this item", masterAddress);
                continue;
            }
            final int port;
            try {
                port = Integer.parseInt(addressItems[1]);
            } catch (NumberFormatException e) {
                LOG.warn("Could not parse port of address {}, ignore this item", masterAddress, e);
                continue;
            }
            addresses.add(Pair.of(addressItems[0], port));
        }
        return addresses;
    }

    public static RowFormatInfo generateRowFormatInfo(FieldInfo[] fieldInfos) {
        int fieldInfosSize = fieldInfos.length;
        String[] fieldNames = new String[fieldInfosSize];
        FormatInfo[] formatInfos = new FormatInfo[fieldInfosSize];
        for (int i = 0; i < fieldInfosSize; ++i) {
            fieldNames[i] = fieldInfos[i].getName();
            formatInfos[i] = fieldInfos[i].getFormatInfo();
        }

        return new RowFormatInfo(fieldNames, formatInfos);
    }

    public static RowSerializer generateRowSerializer(RowFormatInfo rowFormatInfo) {
        FormatInfo[] formatInfos = rowFormatInfo.getFieldFormatInfos();
        TypeInformation<?>[] typeInformations = new TypeInformation<?>[formatInfos.length];
        for (int i = 0; i < formatInfos.length; ++i) {
            typeInformations[i] = TableFormatUtils.getType(formatInfos[i].getTypeInfo());
        }

        return (RowSerializer) new RowTypeInfo(typeInformations).createSerializer(new ExecutionConfig());
    }
}
