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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 单个metric注解
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface Metric {

    /**
     * Type of metric
     *
     * @return metric type
     */
    Type type() default Type.DEFAULT;

    /**
     * Doc of metric
     *
     * @return metric doc
     */
    String desc() default "";

    // metric 类型
    enum Type {
        // 默认类型
        DEFAULT,
        // couter类型，自增
        COUNTER,
        // gauge类型，可上可下
        GAUGE,
        // 标签类型，value是字符串
        TAG
    }
}
