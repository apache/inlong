package org.apache.inlong.agent.plugin.trigger;


import org.apache.inlong.agent.core.trigger.TriggerManager;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.MiniAgent;
import org.apache.inlong.agent.plugin.TestFileAgent;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class TestTriggerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTriggerManager.class);

    private static Path testRootDir;
    private static MiniAgent agent;
    private static AgentBaseTestsHelper helper;
    private static TriggerManager triggerManager;

    @BeforeClass
    public static void setup() {
        try {
            helper = new AgentBaseTestsHelper(TestFileAgent.class.getName()).setupAgentHome();
            agent = new MiniAgent();
            agent.start();
            testRootDir = helper.getTestRootDir();
        } catch (Exception e) {
            LOGGER.error("setup failure");
        }
    }

    // todo:测试重启后trigger的任务是否能正常恢复
    @Test
    public void testRestart() {

    }

    // todo:测试多个不同的trigger监控同一个目录
    @Test
    public void testMultiTriggerWatchSameDir() {}

    // todo:测试trigger超限
    @Test
    public void testExceedMaxNumTrigger() {}

    // todo:测试trigger的提交和停止
    @Test
    public void testSubmitAndShutdown() {}
}
