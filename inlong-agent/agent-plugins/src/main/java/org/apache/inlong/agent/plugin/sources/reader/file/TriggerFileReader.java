package org.apache.inlong.agent.plugin.sources.reader.file;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.validation.constraints.NotNull;

public class TriggerFileReader implements Reader {
    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerFileReader.class);
    @NotNull
    private String triggerId;

    @Override
    public Message read() {
        try {
            // just mock trigger is running
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            // do nothing
        }
        return null;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public String getReadSource() {
        return "Mock file reader trigger " + triggerId;
    }

    @Override
    public void setReadTimeout(long mill) {

    }

    @Override
    public void setWaitMillisecond(long millis) {

    }

    @Override
    public String getSnapshot() {
        return StringUtils.EMPTY;
    }

    @Override
    public void finishRead() {

    }

    @Override
    public boolean isSourceExist() {
        return true;
    }

    @Override
    public void init(JobProfile jobConf) {
        this.triggerId = jobConf.get(JobConstants.JOB_TRIGGER);
    }

    @Override
    public void destroy() {

    }
}
