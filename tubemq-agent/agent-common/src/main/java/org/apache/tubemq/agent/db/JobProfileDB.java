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
package org.apache.tubemq.agent.db;

import static org.apache.tubemq.agent.constants.JobConstants.JOB_ID;

import java.util.ArrayList;
import java.util.List;
import org.apache.tubemq.agent.conf.JobProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for job conf persistence.
 */
public class JobProfileDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobProfileDB.class);

    private static final String JOB_ID_PREFIX = "job_";
    private final DB db;

    public JobProfileDB(DB db) {
        this.db = db;
    }

    /**
     * get job which in accepted state
     * @return null or job conf
     */
    public JobProfile getAcceptedJob() {
        return getJob(StateSearchKey.ACCEPTED);
    }

    public List<JobProfile> getAcceptedJobs() {
        return getJobs(StateSearchKey.ACCEPTED);
    }

    /**
     * update job state and search it by key name
     * @param jobId - job key name
     * @param stateSearchKey - job state
     */
    public void updateJobState(String jobId, StateSearchKey stateSearchKey) {
        KeyValueEntity entity = db.get(JOB_ID_PREFIX + jobId);
        if (entity != null) {
            entity.setStateSearchKey(stateSearchKey);
            db.put(entity);
        }
    }

    /**
     * store job profile
     * @param jobProfile - job profile
     */
    public void storeJob(JobProfile jobProfile) {

        if (jobProfile.allRequiredKeyExist()) {
            String keyName = JOB_ID_PREFIX + jobProfile.get(JOB_ID);
            KeyValueEntity entity = new KeyValueEntity(keyName, jobProfile.toJsonStr());
            db.put(entity);
        }
    }

    public void storeJob(List<JobProfile> jobProfileList) {
        if (jobProfileList != null && jobProfileList.size() > 0) {
            for (JobProfile jobProfile : jobProfileList) {
                storeJob(jobProfile);
            }
        }
    }

    public void deleteJob(String id) {
        String keyName = JOB_ID_PREFIX + id;
        db.remove(keyName);
    }

    /**
     * get job conf by state
     * @param stateSearchKey - state index for searching.
     * @return
     */
    public JobProfile getJob(StateSearchKey stateSearchKey) {
        KeyValueEntity entity = db.searchOne(stateSearchKey);
        if (entity != null) {
            return entity.getAsJobProfile();
        }
        return null;
    }

    /**
     * get list of job profiles.
     * @param stateSearchKey - state search key.
     * @return - list of job profile.
     */
    public List<JobProfile> getJobs(StateSearchKey stateSearchKey) {
        List<KeyValueEntity> entityList = db.search(stateSearchKey);
        List<JobProfile> profileList = new ArrayList<>();
        for (KeyValueEntity entity : entityList) {
            profileList.add(entity.getAsJobProfile());
        }
        return profileList;
    }
}
