package org.apache.inlong.agent.plugin.task;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.plugin.sources.reader.MqttReader;
import org.apache.inlong.agent.utils.AgentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MqttTask extends AbstractTask{

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttTask.class);

    private String topic;

    private boolean isAdded = false;

    public static final String DEFAULT_MQTT_INSTANCE = "org.apache.inlong.agent.plugin.instance.MqttInstance";

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

    @Override
    public boolean isProfileValid(TaskProfile profile) {
        if (!profile.allRequiredKeyExist()) {
            LOGGER.error("task profile needs all required key");
            return false;
        }
        if (!profile.hasKey(profile.get(TaskConstants.JOB_MQTT_TOPIC))) {
            LOGGER.error("task profile needs topic");
            return false;
        }
        if (!profile.hasKey(profile.get(TaskConstants.JOB_MQTT_SERVER_URI))) {
            LOGGER.error("task profile needs serverUri");
            return false;
        }
        if (!profile.hasKey(profile.get(TaskConstants.JOB_MQTT_USERNAME))) {
            LOGGER.error("task profile needs username");
            return false;
        }
        if (!profile.hasKey(profile.get(TaskConstants.JOB_MQTT_PASSWORD))) {
            LOGGER.error("task profile needs password");
            return false;
        }
        return true;
    }

    @Override
    protected int getInstanceLimit() {
        return DEFAULT_INSTANCE_LIMIT;
    }

    @Override
    protected void initTask() {
        LOGGER.info("Mqtt commonInit: {}", taskProfile.toJsonStr());
        topic = taskProfile.get(TaskConstants.JOB_MQTT_TOPIC);
    }

    @Override
    protected List<InstanceProfile> getNewInstanceList() {
        List<InstanceProfile> list = new ArrayList<>();
        if (isAdded) {
            return list;
        }
        String dataTime = LocalDateTime.now().format(dateTimeFormatter);
        InstanceProfile instanceProfile = taskProfile.createInstanceProfile(DEFAULT_MQTT_INSTANCE, topic,
                CycleUnitType.HOUR, dataTime, AgentUtils.getCurrentTime());
        LOGGER.info("taskProfile.createInstanceProfile: {}", instanceProfile.toJsonStr());
        list.add(instanceProfile);
        this.isAdded = true;
        return list;
    }
}
