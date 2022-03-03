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

import static org.junit.Assert.assertEquals;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;
import org.apache.inlong.sort.protocol.transformation.TransformationInfo;
import org.apache.inlong.sort.singletenant.flink.deserialization.ListCollector;
import org.junit.Test;

public class TransformerTest {

    private static final FieldInfo[] sourceFieldInfos = new FieldInfo[] {
            new FieldInfo("name", StringFormatInfo.INSTANCE),
            new FieldInfo("age", IntFormatInfo.INSTANCE),
            new FieldInfo("height", LongFormatInfo.INSTANCE),
    };

    private static final FieldInfo[] sinkFieldInfos = new FieldInfo[] {
            new FieldInfo("age_out", IntFormatInfo.INSTANCE),
            new FieldInfo("name_out", StringFormatInfo.INSTANCE)
    };

    @Test
    public void testTransform() {
        TransformationInfo transformationInfo = new TransformationInfo(
                new FieldMappingRule(new FieldMappingUnit[] {
                        new FieldMappingUnit(
                                new FieldInfo("age", StringFormatInfo.INSTANCE),
                                new FieldInfo("age_out", StringFormatInfo.INSTANCE)),
                        new FieldMappingUnit(
                                new FieldInfo("name", StringFormatInfo.INSTANCE),
                                new FieldInfo("name_out", StringFormatInfo.INSTANCE))
                }));
        Transformer transformer = new Transformer(
                transformationInfo,
                sourceFieldInfos,
                sinkFieldInfos);

        transformer.open(new Configuration());

        Row input = Row.of("name", 29, 179);
        ListCollector<Row> collector = new ListCollector<>();
        transformer.processElement(
                input,
                transformer.new Context() {
                        @Override
                        public Long timestamp() {
                            return null;
                        }

                        @Override
                        public TimerService timerService() {
                            return null;
                        }

                        @Override
                        public <X> void output(OutputTag<X> outputTag, X x) {

                        }
                    },
                collector);
        assertEquals(Row.of(29, "name"), collector.getInnerList().get(0));
    }

    @Test
    public void testTransformWithNotExistSourceFieldName() {
        TransformationInfo transformationInfo = new TransformationInfo(
                new FieldMappingRule(new FieldMappingUnit[] {
                        new FieldMappingUnit(
                                new FieldInfo("age", StringFormatInfo.INSTANCE),
                                new FieldInfo("age_out", StringFormatInfo.INSTANCE)),
                        new FieldMappingUnit(
                                // not exist source field name
                                new FieldInfo("name_not_exist", StringFormatInfo.INSTANCE),
                                new FieldInfo("name_out", StringFormatInfo.INSTANCE))
                }));
        Transformer transformer = new Transformer(
                transformationInfo,
                sourceFieldInfos,
                sinkFieldInfos);

        transformer.open(new Configuration());

        Row input = Row.of("name", 29, 179);
        ListCollector<Row> collector = new ListCollector<>();
        transformer.processElement(
                input,
                transformer.new Context() {
                    @Override
                    public Long timestamp() {
                        return null;
                    }

                    @Override
                    public TimerService timerService() {
                        return null;
                    }

                    @Override
                    public <X> void output(OutputTag<X> outputTag, X x) {

                    }
                },
                collector);
        assertEquals(Row.of(29, null), collector.getInnerList().get(0));
    }

    @Test
    public void testTransformWithNotExistSinkFieldName() {
        TransformationInfo transformationInfo = new TransformationInfo(
                new FieldMappingRule(new FieldMappingUnit[] {
                        new FieldMappingUnit(
                                new FieldInfo("age", StringFormatInfo.INSTANCE),
                                new FieldInfo("age_out", StringFormatInfo.INSTANCE)),
                        new FieldMappingUnit(
                                new FieldInfo("name", StringFormatInfo.INSTANCE),
                                // not exist sink field name
                                new FieldInfo("name_out_not_exist", StringFormatInfo.INSTANCE))
                }));
        Transformer transformer = new Transformer(
                transformationInfo,
                sourceFieldInfos,
                sinkFieldInfos);

        transformer.open(new Configuration());

        Row input = Row.of("name", 29, 179);
        ListCollector<Row> collector = new ListCollector<>();
        transformer.processElement(
                input,
                transformer.new Context() {
                    @Override
                    public Long timestamp() {
                        return null;
                    }

                    @Override
                    public TimerService timerService() {
                        return null;
                    }

                    @Override
                    public <X> void output(OutputTag<X> outputTag, X x) {

                    }
                },
                collector);
        assertEquals(Row.of(29, null), collector.getInnerList().get(0));
    }

}
