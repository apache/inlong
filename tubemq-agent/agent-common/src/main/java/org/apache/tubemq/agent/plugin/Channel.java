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
package org.apache.tubemq.agent.plugin;

import java.util.concurrent.TimeUnit;

/**
 * Channel is used as data buffer between source and sink.
 */
public interface Channel extends Stage {


    /**
     * Push data into
     *
     * @param message - 待写入消息
     */
    void push(Message message);

    /**
     * 提供给reader接口写入数据，如果channel缓冲满了，等待队列资源 如果message为null，则忽略写入
     *
     * @param message - 待写入消息
     */
    boolean push(Message message, long timeout, TimeUnit unit);

    /**
     * 提供给writer接口读取数据，如果channel缓冲池空，则等待新数据，超过一定的时限则返回null
     *
     * @return - 返回单条message
     */
    Message pull(long timeout, TimeUnit unit);

}
