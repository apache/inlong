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
import org.apache.inlong.manager.pojo.sort.node.base.LoadNodeProvider;
import org.apache.inlong.manager.pojo.sort.node.load.ClickHouseProvider;
import org.apache.inlong.manager.pojo.sort.node.load.DorisProvider;
import org.apache.inlong.manager.pojo.sort.node.load.ElasticsearchProvider;
import org.apache.inlong.manager.pojo.sort.node.load.GreenplumProvider;
import org.apache.inlong.manager.pojo.sort.node.load.HBaseProvider;
import org.apache.inlong.manager.pojo.sort.node.load.HDFSProvider;
import org.apache.inlong.manager.pojo.sort.node.load.HiveProvider;
import org.apache.inlong.manager.pojo.sort.node.load.HudiProvider;
import org.apache.inlong.manager.pojo.sort.node.load.IcebergProvider;
import org.apache.inlong.manager.pojo.sort.node.load.KafkaProvider;
import org.apache.inlong.manager.pojo.sort.node.load.KuduProvider;
import org.apache.inlong.manager.pojo.sort.node.load.MySQLProvider;
import org.apache.inlong.manager.pojo.sort.node.load.OracleProvider;
import org.apache.inlong.manager.pojo.sort.node.load.PostgreSQLProvider;
import org.apache.inlong.manager.pojo.sort.node.load.RedisProvider;
import org.apache.inlong.manager.pojo.sort.node.load.SQLServerProvider;
import org.apache.inlong.manager.pojo.sort.node.load.StarRocksProvider;
import org.apache.inlong.manager.pojo.sort.node.load.TDSQLPostgreSQLProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory of the load node provider.
 */
public class LoadNodeProviderFactory {

    /**
     * The load node provider collection
     */
    private static final List<LoadNodeProvider> LOAD_NODE_PROVIDER_LIST = new ArrayList<>();

    static {
        // The Providers Parsing SinkInfo to LoadNode which sort needed
        LOAD_NODE_PROVIDER_LIST.add(new KafkaProvider());
        LOAD_NODE_PROVIDER_LIST.add(new ClickHouseProvider());
        LOAD_NODE_PROVIDER_LIST.add(new DorisProvider());
        LOAD_NODE_PROVIDER_LIST.add(new ElasticsearchProvider());
        LOAD_NODE_PROVIDER_LIST.add(new GreenplumProvider());
        LOAD_NODE_PROVIDER_LIST.add(new HBaseProvider());
        LOAD_NODE_PROVIDER_LIST.add(new HDFSProvider());
        LOAD_NODE_PROVIDER_LIST.add(new HiveProvider());
        LOAD_NODE_PROVIDER_LIST.add(new HudiProvider());
        LOAD_NODE_PROVIDER_LIST.add(new IcebergProvider());
        LOAD_NODE_PROVIDER_LIST.add(new KuduProvider());
        LOAD_NODE_PROVIDER_LIST.add(new MySQLProvider());
        LOAD_NODE_PROVIDER_LIST.add(new OracleProvider());
        LOAD_NODE_PROVIDER_LIST.add(new PostgreSQLProvider());
        LOAD_NODE_PROVIDER_LIST.add(new RedisProvider());
        LOAD_NODE_PROVIDER_LIST.add(new SQLServerProvider());
        LOAD_NODE_PROVIDER_LIST.add(new StarRocksProvider());
        LOAD_NODE_PROVIDER_LIST.add(new TDSQLPostgreSQLProvider());
    }

    /**
     * Get load node provider
     *
     * @param sinkType the specified sink type
     * @return the load node provider
     */
    public static LoadNodeProvider getLoadNodeProvider(String sinkType) {
        return LOAD_NODE_PROVIDER_LIST.stream()
                .filter(inst -> inst.accept(sinkType))
                .findFirst()
                .orElseThrow(() -> new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                        String.format(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage(), sinkType)));
    }
}
