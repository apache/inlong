package org.apache.inlong.agent.plugin.utils;

import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.db.RocksDbImp;
import org.apache.inlong.agent.db.TriggerProfileDb;
import org.apache.inlong.agent.utils.AgentUtils;

import java.util.List;

import static org.apache.inlong.agent.constant.JobConstants.JOB_ID_PREFIX;

public class RocksDBUtils {

    public static void main(String[] args) {
        // 找到所有trigger给其打上job.instance.id的符号
        // 找到所有的pattern，给其替换成patterns然后再弄上去
        Db db = new RocksDbImp();
        upgrade(db);
    }

    public static void upgrade(Db db) {
        TriggerProfileDb triggerProfileDb = new TriggerProfileDb(db);
        List<TriggerProfile> allTriggerProfiles = triggerProfileDb.getTriggers();
        allTriggerProfiles.forEach(triggerProfile -> {
            if (triggerProfile.hasKey(JobConstants.JOB_DIR_FILTER_PATTERN)) {
                triggerProfile.set(JobConstants.JOB_DIR_FILTER_PATTERNS,
                        triggerProfile.get(JobConstants.JOB_DIR_FILTER_PATTERN));
                triggerProfile.set(JobConstants.JOB_DIR_FILTER_PATTERN, null);
            }

            triggerProfile.set(JobConstants.JOB_INSTANCE_ID,
                    AgentUtils.getSingleJobId(JOB_ID_PREFIX, triggerProfile.getTriggerId()));

            triggerProfileDb.storeTrigger(triggerProfile);
        });
    }

    public static void printTrigger(Db db) {
    }
}
