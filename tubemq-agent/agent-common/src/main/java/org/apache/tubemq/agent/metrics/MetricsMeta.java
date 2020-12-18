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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.tubemq.agent.metrics.counter.Counter;
import org.apache.tubemq.agent.metrics.gauge.Gauge;

/**
 * 此类和{@link Metrics} 关联对应
 */
public class MetricsMeta {

    private String name;
    private String context;
    private String desc;

    // 被Metrics注解的类实例
    private Object source;

    private List<FieldMetricMeta> fields;
    private List<MethodMetricMeta> methods;

    private MetricsMeta() {
    }

    public static MetricsMeta build(Object source, Metrics metrics,
            List<FieldMetricMeta> fields,
            List<MethodMetricMeta> methods) {
        MetricsMeta metricsMeta = new MetricsMeta();
        String name = metrics.name();
        if (StringUtils.isEmpty(name)) {
            name = source.getClass().getSimpleName();
        }
        metricsMeta.name = StringUtils.capitalize(name);
        metricsMeta.context = metrics.context();
        metricsMeta.desc = metrics.desc();

        metricsMeta.source = source;
        metricsMeta.fields = fields;
        metricsMeta.methods = methods;
        return metricsMeta;
    }

    public String getName() {
        return name;
    }

    public String getContext() {
        return context;
    }

    public String getDesc() {
        return desc;
    }

    public Object getSource() {
        return source;
    }

    public List<FieldMetricMeta> getFields() {
        return fields;
    }

    public List<MethodMetricMeta> getMethods() {
        return methods;
    }

    /**
     * 此类和{@link Metric} 关联对应
     */
    private static class MetricMeta {

        String name;
        Metric.Type type;
        String desc;

        static Metric.Type getType(Class<?> clz, Metric.Type defaultType) {
            if (clz.isAssignableFrom(Counter.class)) {
                return Metric.Type.COUNTER;
            } else if (clz.isAssignableFrom(Gauge.class)) {
                return Metric.Type.GAUGE;
            } else {
                return defaultType;
            }
        }

        public Metric.Type getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public String getDesc() {
            return desc;
        }
    }

    /**
     * 处理field级别的注解
     */
    public static class FieldMetricMeta extends MetricMeta {

        private Field field;


        public static FieldMetricMeta build(Metric annotation, Field field) {
            FieldMetricMeta meta = new FieldMetricMeta();
            meta.name = StringUtils.capitalize(field.getName());
            meta.type = getType(field.getType(), annotation.type());
            meta.desc = annotation.desc();

            meta.field = field;
            return meta;
        }

        public Field getField() {
            return field;
        }
    }

    /**
     * 处理method级别的注解
     */
    public static class MethodMetricMeta extends MetricMeta {

        private Method method;


        public static MethodMetricMeta build(Metric annotation, Method method) {
            MethodMetricMeta meta = new MethodMetricMeta();
            String methodName = method.getName();
            if (methodName.startsWith("get")) {
                methodName = methodName.substring(3);
            }
            meta.name = StringUtils.capitalize(methodName);
            meta.type = getType(method.getReturnType(), annotation.type());
            meta.desc = annotation.desc();
            meta.method = method;
            return meta;
        }

        public Method getMethod() {
            return method;
        }
    }
}
