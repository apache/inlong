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

import java.util.concurrent.atomic.AtomicLong;

public class GaugeLong implements Gauge {

    private AtomicLong value = new AtomicLong(0);

    @Override
    public void incr() {
        value.incrementAndGet();
    }

    @Override
    public void incr(int delta) {
        assert delta > 0;
        value.getAndAdd(delta);
    }

    @Override
    public void decr() {
        value.decrementAndGet();
    }

    @Override
    public void decr(int delta) {
        assert delta > 0;
        value.getAndAdd(-delta);
    }

    @Override
    public Long snapshot() {
        return value.get();
    }
}
