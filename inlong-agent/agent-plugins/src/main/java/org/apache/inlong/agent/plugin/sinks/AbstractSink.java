package org.apache.inlong.agent.plugin.sinks;

import static org.apache.inlong.agent.constants.AgentConstants.AGENT_MESSAGE_FILTER_CLASSNAME;

import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.plugin.MessageFilter;
import org.apache.inlong.agent.plugin.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract Sink to implement some common method
 * @Author pengzirui
 * @Date 2021/10/14 2:27 下午
 * @Version 1.0
 */
public abstract class AbstractSink implements Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentManager.class);

    @Override
    public MessageFilter initMessageFilter(JobProfile jobConf) {
        if (jobConf.hasKey(AGENT_MESSAGE_FILTER_CLASSNAME)) {
            try {
                return (MessageFilter) Class.forName(jobConf.get(AGENT_MESSAGE_FILTER_CLASSNAME))
                    .getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                LOGGER.error("init message filter error", e);
            }
        }
        return null;
    }

}
