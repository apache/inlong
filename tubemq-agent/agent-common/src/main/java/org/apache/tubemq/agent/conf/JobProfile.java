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
package org.apache.tubemq.agent.conf;

import static org.apache.tubemq.agent.constants.JobConstants.JOB_CHANNEL;
import static org.apache.tubemq.agent.constants.JobConstants.JOB_ID;
import static org.apache.tubemq.agent.constants.JobConstants.JOB_NAME;
import static org.apache.tubemq.agent.constants.JobConstants.JOB_SINK;
import static org.apache.tubemq.agent.constants.JobConstants.JOB_SOURCE;

import com.google.gson.Gson;

/**
 * job profile which contains details describing properties of one job.
 *
 */
public class JobProfile extends Configuration {

    private final Gson gson = new Gson();

    /**
     * parse json string to configuration instance。
     *
     * @param jsonStr
     * @return job configuration
     */
    public static JobProfile parseJsonStr(String jsonStr) {
        JobProfile conf = new JobProfile();
        conf.loadJsonStrResource(jsonStr);
        return conf;
    }

    /**
     * parse properties file
     *
     * @param fileName - file name.
     * @return jobConfiguration.
     */
    public static JobProfile parsePropertiesFile(String fileName) {
        JobProfile conf = new JobProfile();
        conf.loadPropertiesResource(fileName);
        return conf;
    }

    /**
     * pase json file.
     * @param fileName - json file name.
     * @return jobConfiguration.
     */
    public static JobProfile parseJsonFile(String fileName) {
        JobProfile conf = new JobProfile();
        conf.loadJsonResource(fileName);
        return conf;
    }

    /**
     * 检查所有必填字段是否存在
     *
     * @return 如果必填字段缺失返回false，否则返回true
     */
    public boolean allRequiredKeyExist() {
        return hasKey(JOB_ID) && hasKey(JOB_SOURCE)
                && hasKey(JOB_SINK) && hasKey(JOB_CHANNEL) && hasKey(JOB_NAME);
    }

    public String toJsonStr() {
        return gson.toJson(getConfigStorage());
    }
}
