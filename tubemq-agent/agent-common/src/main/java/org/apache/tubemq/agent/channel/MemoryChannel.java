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
package org.apache.tubemq.agent.channel;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.plugin.Channel;
import org.apache.tubemq.agent.plugin.Message;

public class MemoryChannel implements Channel {

    private LinkedBlockingQueue<Message> queue;

    /**
     * {@inheritDoc}
     */
    @Override
    public void push(Message message) {
        try {
            if (message != null) {
                queue.put(message);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean push(Message message, long timeout, TimeUnit unit) {
        try {
            if (message != null) {
                return queue.offer(message, timeout, unit);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message pull(long timeout, TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void init(JobProfile jobConf) {
        queue = new LinkedBlockingQueue<>(
                jobConf.getInt(
                    AgentConstants.CHANNEL_MEMORY_CAPACITY, AgentConstants.DEFAULT_CHANNEL_MEMORY_CAPACITY));
    }

    @Override
    public void destroy() {
        if (queue != null) {
            queue.clear();
        }
    }
}
