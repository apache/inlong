/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.metrics;

import static org.apache.tubemq.agent.metrics.MetricsMeta.FieldMetricMeta;
import static org.apache.tubemq.agent.metrics.MetricsMeta.MethodMetricMeta;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tubemq.agent.metrics.counter.CounterInt;
import org.apache.tubemq.agent.metrics.counter.CounterLong;
import org.apache.tubemq.agent.metrics.gauge.GaugeInt;
import org.apache.tubemq.agent.metrics.gauge.GaugeLong;
import org.apache.tubemq.agent.utils.AgentUtils;

/**
 * metric register
 */
public class MetricsRegister {

    private static ConcurrentHashMap<String, MetricsMeta> sources = new ConcurrentHashMap<>();

    private MetricsRegister() {
    }


    /**
     * register
     *
     * @param source metrics
     */
    public static void register(Object source) {

        List<FieldMetricMeta> fields = handleFieldAnnotation(source);

        List<MethodMetricMeta> methods = handleMethodAnnotation(source);

        handleClassAnnotation(source, fields, methods);
    }

    /**
     * class annotation
     */
    private static void handleClassAnnotation(Object source,
            List<FieldMetricMeta> fields,
            List<MethodMetricMeta> methods) {
        for (Annotation annotation : source.getClass().getAnnotations()) {
            if (annotation instanceof Metrics) {
                MetricsMeta metricsMeta = MetricsMeta.build(source,
                        (Metrics) annotation, fields, methods);
                sources.putIfAbsent(metricsMeta.getName(), metricsMeta);
                break;
            }
        }
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

    /**
     * handle fields
     */
    private static List<FieldMetricMeta> handleFieldAnnotation(Object source) {
        List<FieldMetricMeta> result = new ArrayList<>();
        for (Field field : AgentUtils.getDeclaredFieldsIncludingInherited(source.getClass())) {

            // metric accessible
            field.setAccessible(true);
            for (Annotation fieldAnnotation : field.getAnnotations()) {
                if (fieldAnnotation instanceof Metric) {
                    if (initFieldByType(source, field)) {
                        result.add(FieldMetricMeta.build((Metric) fieldAnnotation, field));
                    }
                    break;
                }
            }
        }
        return result;
    }

    /**
     * handle methods
     *
     * @throws MetricException npe
     */
    private static List<MethodMetricMeta> handleMethodAnnotation(Object source) {
        List<MethodMetricMeta> result = new ArrayList<>();
        for (Method method : AgentUtils.getDeclaredMethodsIncludingInherited(source.getClass())) {


            if (method.isVarArgs()) {
                throw new MetricException("Cannot declare on method with arguments");
            }

            method.setAccessible(true);
            for (Annotation methodAnnotation : method.getAnnotations()) {
                if (methodAnnotation instanceof Metric) {
                    // TODO: handle method column
                    result.add(MethodMetricMeta.build((Metric) methodAnnotation, method));
                    break;
                }
            }
        }
        return result;
    }


}

