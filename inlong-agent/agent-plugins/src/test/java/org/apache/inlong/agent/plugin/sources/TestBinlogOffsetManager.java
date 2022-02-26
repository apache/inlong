package org.apache.inlong.agent.plugin.sources;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.inlong.agent.plugin.AgentBaseTestsHelper;
import org.apache.inlong.agent.plugin.sources.snapshot.BinlogSnapshotManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBinlogOffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTextFileReader.class);
    private static Path testDir;
    private static AgentBaseTestsHelper helper;

    @BeforeClass
    public static void setup() {
        helper = new AgentBaseTestsHelper(TestTextFileReader.class.getName()).setupAgentHome();
        testDir = helper.getTestRootDir();
    }

    @AfterClass
    public static void teardown() {
        helper.teardownAgentHome();
    }

    @Test
    public void testOffset() {
        BinlogSnapshotManager snapshotManager = new BinlogSnapshotManager(testDir.toString());
        byte[] snapshotBytes = new byte[]{-65, -14, -23};
        String snapshotString = new String(snapshotBytes, StandardCharsets.ISO_8859_1);
        snapshotManager.save(snapshotString);
        Assert.assertEquals(snapshotManager.getSnapshot(), snapshotString);
    }

}
