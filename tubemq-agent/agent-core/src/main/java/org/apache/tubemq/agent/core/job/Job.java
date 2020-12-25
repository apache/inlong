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
package org.apache.tubemq.agent.core.job;


import java.util.ArrayList;
import java.util.List;
import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.constants.JobConstants;
import org.apache.tubemq.agent.core.task.Task;
import org.apache.tubemq.agent.plugin.Channel;
import org.apache.tubemq.agent.plugin.Reader;
import org.apache.tubemq.agent.plugin.Sink;
import org.apache.tubemq.agent.plugin.Source;

/**
 * job meta definition, job will be split into several tasks.
 */
public class Job {

    private final JobProfile jobConf;
    // job name
    private String name;
    // job description
    private String description;
    private String jobId;

    public Job(JobProfile jobConf) {
        this.jobConf = jobConf;
        this.name = jobConf.get(JobConstants.JOB_NAME, JobConstants.DEFAULT_JOB_NAME);
        this.description = jobConf.get(
            JobConstants.JOB_DESCRIPTION, JobConstants.DEFAULT_JOB_DESCRIPTION);
        this.jobId = jobConf.get(JobConstants.JOB_ID);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public List<Task> createTasks() {
        List<Task> taskList = new ArrayList<>();
        int index = 0;
        try {
            Source source = (Source) Class.forName(jobConf.get(JobConstants.JOB_SOURCE)).newInstance();
            for (Reader reader : source.split(jobConf)) {
                Sink writer = (Sink) Class.forName(jobConf.get(JobConstants.JOB_SINK)).newInstance();
                Channel channel = (Channel) Class.forName(jobConf.get(JobConstants.JOB_CHANNEL)).newInstance();
                String taskId = String.format("%s_%d", jobId, index++);
                taskList.add(new Task(taskId, reader, writer, channel, getJobConf()));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return taskList;
    }

    public JobProfile getJobConf() {
        return this.jobConf;
    }

}
