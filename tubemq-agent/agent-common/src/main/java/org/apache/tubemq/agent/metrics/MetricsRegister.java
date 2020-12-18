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
 * metric注册类，这个类被单例调用，因此无需额外方法保证线程安全
 */
public class MetricsRegister {

    private static ConcurrentHashMap<String, MetricsMeta> sources = new ConcurrentHashMap<>();

    private MetricsRegister() {
    }


    /**
     * 注册metrics类，
     *
     * @param source metrics实现类实例
     */
    public static void register(Object source) {

        // 字段注解
        List<FieldMetricMeta> fields = handleFieldAnnotation(source);

        // 方法注解
        List<MethodMetricMeta> methods = handleMethodAnnotation(source);

        // 类注解
        handleClassAnnotation(source, fields, methods);
    }

    /**
     * 处理类注解
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
     * 处理字段注解
     */
    private static List<FieldMetricMeta> handleFieldAnnotation(Object source) {
        List<FieldMetricMeta> result = new ArrayList<>();
        for (Field field : AgentUtils.getDeclaredFieldsIncludingInherited(source.getClass())) {

            // metric字段统一可访问
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
     * 处理方法注解
     *
     * @throws MetricException 如果注解放在需要传参的方法上，则抛出异常
     */
    private static List<MethodMetricMeta> handleMethodAnnotation(Object source) {
        List<MethodMetricMeta> result = new ArrayList<>();
        for (Method method : AgentUtils.getDeclaredMethodsIncludingInherited(source.getClass())) {

            // 需要传参的方法如果有注解直接抛出异常
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

