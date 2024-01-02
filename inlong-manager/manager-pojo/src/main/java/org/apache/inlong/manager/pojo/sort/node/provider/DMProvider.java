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

package org.apache.inlong.manager.pojo.sort.node.provider;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.consts.StreamType;
import org.apache.inlong.manager.common.fieldtype.strategy.DamengFieldTypeStrategy;
import org.apache.inlong.manager.common.fieldtype.strategy.FieldTypeMappingStrategy;
import org.apache.inlong.manager.common.fieldtype.strategy.MySQLFieldTypeStrategy;
import org.apache.inlong.manager.pojo.sort.node.base.ExtractNodeProvider;
import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.source.dameng.DamengSource;
import org.apache.inlong.manager.pojo.source.mysql.MySQLBinlogSource;
import org.apache.inlong.manager.pojo.source.oracle.OracleSource;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.constant.DMConstant;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.DamengExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;

import com.google.common.base.Splitter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Provider for creating MySQLBinlog extract nodes.
 */
public class DMProvider implements ExtractNodeProvider {

    private static final FieldTypeMappingStrategy FIELD_TYPE_MAPPING_STRATEGY = new DamengFieldTypeStrategy();

    @Override
    public Boolean accept(String sourceType) {
        return SourceType.DAMENG.equals(sourceType);
    }

    @Override
    public ExtractNode createExtractNode(StreamNode streamNodeInfo) {
        DamengSource source = (DamengSource) streamNodeInfo;
        List<FieldInfo> fieldInfos = parseStreamFieldInfos(source.getFieldList(), source.getSourceName(),
                FIELD_TYPE_MAPPING_STRATEGY);
        DMConstant.ScanStartUpMode scanStartupMode = StringUtils.isBlank(source.getScanStartupMode())
                ? null
                : DMConstant.ScanStartUpMode.forName(source.getScanStartupMode());
        Map<String, String> properties = parseProperties(source.getProperties());

        return new DamengExtractNode(
                source.getSourceName(),
                source.getSourceName(),
                fieldInfos,
                null,
                properties,
                source.getPrimaryKey(),
                source.getHost(),
                source.getUsername(),
                source.getPassword(),
                source.getDbName(),
                source.getSchemaName(),
                source.getTableName(),
                source.getPort(),
                scanStartupMode);
    }

}