/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.common.metrics.meta;

import static org.apache.inlong.common.metrics.Metric.Type.COUNTER_INT;
import static org.apache.inlong.common.metrics.Metric.Type.COUNTER_LONG;
import static org.apache.inlong.common.metrics.Metric.Type.DEFAULT;
import static org.apache.inlong.common.metrics.Metric.Type.GAUGE_INT;
import static org.apache.inlong.common.metrics.Metric.Type.GAUGE_LONG;

import java.lang.reflect.Field;
import org.apache.inlong.common.metrics.Metric;
import org.apache.inlong.common.metrics.counter.CounterInt;
import org.apache.inlong.common.metrics.counter.CounterLong;
import org.apache.inlong.common.metrics.gauge.GaugeInt;
import org.apache.inlong.common.metrics.gauge.GaugeLong;

/**
 * this class is related to {@link Metric}
 */
public class MetricMeta {

    private String name;
    private String type;
    private String desc;
    private Field field;

    /**
     * build metrics
     *
     * @param annotation
     * @param field
     * @return
     */
    public static MetricMeta build(Metric annotation, Field field) {
        MetricMeta metricMeta = new MetricMeta();

        metricMeta.name = capitalize(field.getName());
        metricMeta.desc = annotation.desc();
        metricMeta.type = DEFAULT.getValue();
        metricMeta.field = field;
        Class<?> clz = field.getType();
        if (clz.isAssignableFrom(CounterLong.class)) {
            metricMeta.type = COUNTER_LONG.getValue();
        } else if (clz.isAssignableFrom(CounterInt.class)) {
            metricMeta.type = COUNTER_INT.getValue();
        } else if (clz.isAssignableFrom(GaugeInt.class)) {
            metricMeta.type = GAUGE_INT.getValue();
        } else if (clz.isAssignableFrom(GaugeLong.class)) {
            metricMeta.type = GAUGE_LONG.getValue();
        }
        return metricMeta;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }

    public Field getField() {
        return field;
    }

    public static String capitalize(String str) {
        int strLen;
        return str != null && (strLen = str.length()) != 0
                ? (new StringBuilder(strLen)).append(Character.toTitleCase(str.charAt(0)))
                        .append(str.substring(1)).toString() : str;
    }
}

