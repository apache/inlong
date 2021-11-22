package org.apache.inlong.agent.core;

import org.apache.inlong.agent.core.task.TaskMetrics;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentBaseTestsHelper.class);

    @Test
    public void testAgentMetrics() {
        try {
            TaskMetrics taskMetrics = TaskMetrics.create();
            taskMetrics.retryingTasks.addAndGet(1);
            Assert.assertEquals(taskMetrics.module, "AgentTaskMetric");
        } catch (Exception ex) {
            LOGGER.error("error happens" + ex);
        }
    }

}
