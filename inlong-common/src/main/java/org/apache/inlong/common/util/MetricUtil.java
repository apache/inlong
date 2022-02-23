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

package org.apache.inlong.common.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.inlong.common.metrics.MetricException;
import org.apache.inlong.common.metrics.ResourceUsage;
import org.apache.inlong.common.metrics.Tag;
import org.apache.inlong.common.metrics.counter.CounterInt;
import org.apache.inlong.common.metrics.counter.CounterLong;
import org.apache.inlong.common.metrics.gauge.GaugeInt;
import org.apache.inlong.common.metrics.gauge.GaugeLong;

public class MetricUtil {
    public static final long MIBI = 1024 * 1024L;
    private static final int HUNDRED_PERCENT = 100;

    /**
     * Get declare fields.
     */
    public static List<Field> getDeclaredFieldsIncludingInherited(Class<?> clazz) {
        List<Field> fields = new ArrayList<Field>();
        // check whether parent exists
        while (clazz != null) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    private static boolean initFieldByType(Object source, Field field) {
        try {
            if (field.getType() == CounterInt.class) {
                field.set(source, new CounterInt());
                return true;
            } else if (field.getType() == CounterLong.class) {
                field.set(source, new CounterLong());
                return true;
            } else if (field.getType() == GaugeInt.class) {
                field.set(source, new GaugeInt());
                return true;
            } else if (field.getType() == GaugeLong.class) {
                field.set(source, new GaugeLong());
                return true;
            } else if (field.getType() == Tag.class) {
                field.set(source, new Tag());
                return true;
            } else {
                throw new MetricException("field type error " + field.getType().toString());
            }
        } catch (MetricException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new MetricException("Error setting field " + field
                    + " annotated with metric", ex);
        }
    }

    public static ResourceUsage getHeapMemLoadInfo() {
        double maxHeapMemoryInMBs = (double) Runtime.getRuntime().maxMemory() / MIBI;
        double memoryUsageInMBs = (double) (Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory()) / MIBI;
        return new ResourceUsage(memoryUsageInMBs,
                maxHeapMemoryInMBs,
                memoryUsageInMBs / maxHeapMemoryInMBs * HUNDRED_PERCENT);
    }

    public static int getUsagePercentage() {
        double maxHeapMemoryInMBs = (double) Runtime.getRuntime().maxMemory() / MIBI;
        double memoryUsageInMBs = (double) (Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory()) / MIBI;
        return (int) (memoryUsageInMBs / maxHeapMemoryInMBs * 100);
    }
}
