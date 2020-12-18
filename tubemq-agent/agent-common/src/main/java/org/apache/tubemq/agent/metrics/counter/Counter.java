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
package org.apache.tubemq.agent.metrics.counter;

import org.apache.tubemq.agent.metrics.MutableMetric;

public interface Counter extends MutableMetric {

    /**
     * 自加+1方法
     */
    void incr();

    /**
     * 自加+delta方法
     *
     * @param delta 正数 > 0
     */
    void incr(int delta);

}
