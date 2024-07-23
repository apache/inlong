package org.apache.inlong.agent.plugin.instance;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.constant.TaskConstants;

import java.io.IOException;

public class MqttInstance extends CommonInstance{
    @Override
    public void setInodeInfo(InstanceProfile profile){
        profile.set(TaskConstants.INODE_INFO, "");
    }
}
