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

package org.apache.inlong.manager.pojo.sort.node;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.HudiProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.KafkaProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.MongoProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.MySQLBinlogProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.OracleProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.PostgreSQLProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.PulsarProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.RedisProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.SQLServerProvider;
import org.apache.inlong.manager.pojo.sort.node.extract.TubeMqProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory of the extract node provider
 */
public class ExtractNodeProviderFactory {

    /**
     * The extract node provider collection
     */
    private static final List<ExtractNodeProvider> EXTRACT_NODE_PROVIDER_LIST = new ArrayList<>();

    static {
        // The Providers Parsing SourceInfo to ExtractNode which sort needed
        EXTRACT_NODE_PROVIDER_LIST.add(new HudiProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new KafkaProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new MongoProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new OracleProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new PulsarProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new RedisProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new TubeMqProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new SQLServerProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new PostgreSQLProvider());
        EXTRACT_NODE_PROVIDER_LIST.add(new MySQLBinlogProvider());

    }

    /**
     * Get extract node provider
     *
     * @param sourceType the specified source type
     * @return the extract node provider
     */
    public static ExtractNodeProvider getExtractNodeProvider(String sourceType) {
        return EXTRACT_NODE_PROVIDER_LIST.stream()
                .filter(inst -> inst.accept(sourceType))
                .findFirst()
                .orElseThrow(() -> new BusinessException(ErrorCodeEnum.SOURCE_TYPE_NOT_SUPPORT,
                        String.format(ErrorCodeEnum.SOURCE_TYPE_NOT_SUPPORT.getMessage(), sourceType)));
    }
}
