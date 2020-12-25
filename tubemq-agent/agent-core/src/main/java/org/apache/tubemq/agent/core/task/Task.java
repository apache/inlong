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
package org.apache.tubemq.agent.core.task;

import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.plugin.Channel;
import org.apache.tubemq.agent.plugin.Reader;
import org.apache.tubemq.agent.plugin.Sink;


/**
 * task meta definition which contains reader -> channel -> sink and job config information
 */
public class Task {

    private final String taskId;
    private final Reader reader;
    private final Sink sink;
    private final Channel channel;
    private final JobProfile jobConf;

    public Task(String taskId, Reader reader, Sink sink, Channel channel,
            JobProfile jobConf) {
        this.reader = reader;
        this.sink = sink;
        this.taskId = taskId;
        this.channel = channel;
        this.jobConf = jobConf;
    }

    public boolean isReadFinished() {
        return reader.isFinished();
    }

    public String getTaskId() {
        return taskId;
    }

    public Reader getReader() {
        return reader;
    }

    public Sink getSink() {
        return sink;
    }

    public Channel getChannel() {
        return channel;
    }

    public void init() {
        this.channel.init(jobConf);
        this.sink.init(jobConf);
        this.reader.init(jobConf);
    }

    public void destroy() {
        this.reader.destroy();
        this.sink.destroy();
        this.channel.destroy();
    }
}
