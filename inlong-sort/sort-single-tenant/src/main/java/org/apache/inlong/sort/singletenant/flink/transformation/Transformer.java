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

package org.apache.inlong.sort.singletenant.flink.transformation;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;
import org.apache.inlong.sort.protocol.transformation.TransformationInfo;
import org.apache.inlong.sort.protocol.transformation.TransformationRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transformer extends ProcessFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transformer.class);

    private final TransformationInfo transformationInfo;

    private final FieldInfo[] sourceFieldInfos;

    private final FieldInfo[] sinkFieldInfos;

    private transient Function<Row, Row> rowTransformer;

    public Transformer(
            TransformationInfo transformationInfo,
            FieldInfo[] sourceFieldInfos,
            FieldInfo[] sinkFieldInfos) {
        this.transformationInfo = Preconditions.checkNotNull(transformationInfo);
        this.sourceFieldInfos = Preconditions.checkNotNull(sourceFieldInfos);
        this.sinkFieldInfos = Preconditions.checkNotNull(sinkFieldInfos);
    }

    @Override
    public void open(Configuration parameters) {
        rowTransformer = getRowTransformer();
    }

    @Override
    public void processElement(
            Row input,
            ProcessFunction<Row, Row>.Context context,
            Collector<Row> collector) {
        collector.collect(rowTransformer.apply(input));
    }

    private Function<Row, Row> getRowTransformer() {
        TransformationRule transformationRule = transformationInfo.getTransformRule();
        if (transformationRule instanceof FieldMappingRule) {
            // Get 'sink field name' => 'source field name' map
            FieldMappingRule fieldMappingRule = (FieldMappingRule) transformationRule;
            final Map<String, String> sinkFieldNameToSourceFieldName = new HashMap<>();
            for (FieldMappingUnit fieldMappingUnit : fieldMappingRule.getFieldMappingUnits()) {
                sinkFieldNameToSourceFieldName.put(
                        fieldMappingUnit.getSinkFieldInfo().getName(),
                        fieldMappingUnit.getSourceFieldInfo().getName());
            }

            // Get 'source field name' => 'index in source row' map
            final Map<String, Integer> sourceFieldNameToIndex = new HashMap<>();
            for (int i = 0; i < sourceFieldInfos.length; i++) {
                sourceFieldNameToIndex.put(sourceFieldInfos[i].getName(), i);
            }

            return row -> {
                final int sinkFieldsLength = sinkFieldInfos.length;
                Row sinkRow = new Row(sinkFieldsLength);
                sinkRow.setKind(row.getKind());
                for (int i = 0; i < sinkFieldsLength; i++) {
                    String sinkFieldName = sinkFieldInfos[i].getName();
                    String sourceFieldName = sinkFieldNameToSourceFieldName.get(sinkFieldName);

                    if (sourceFieldName == null) {
                        LOGGER.warn("Mapping failed! Can't find correspond source field! sink field name `{}`",
                                sinkFieldName);
                        sinkRow.setField(i, null);
                        continue;
                    }

                    Integer sourceFieldIndex = sourceFieldNameToIndex.get(sourceFieldName);
                    if (sourceFieldIndex == null) {
                        LOGGER.warn("Mapping failed! Can't find correspond source field!"
                                + " source field name `{}`, sink field name `{}`", sourceFieldName, sinkFieldName);
                        sinkRow.setField(i, null);
                        continue;
                    }

                    sinkRow.setField(i, row.getField(sourceFieldIndex));
                }
                return sinkRow;
            };
        } else {
            throw new IllegalArgumentException("Unsupported transformation rule " + transformationRule.getClass());
        }
    }
}
