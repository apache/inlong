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
package org.apache.tubemq.agent.metrics.gauge;


import org.apache.tubemq.agent.metrics.MutableMetric;

public interface Gauge extends MutableMetric {

    /**
     * increments
     */
    void incr();

    /**
     * increments with delta
     *
     * @param delta  > 0
     */
    void incr(int delta);

    /**
     * decrements
     */
    void decr();

    /**
     * decrements with delta
     *
     * @param delta > 0
     */
    void decr(int delta);
}
