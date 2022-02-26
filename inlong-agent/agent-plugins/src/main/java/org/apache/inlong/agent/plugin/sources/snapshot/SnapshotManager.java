package org.apache.inlong.agent.plugin.sources.snapshot;

public interface SnapshotManager {

    /**
     * get snapshot of the job
     * @return
     */
    String getSnapshot();

    /**
     * close resources
     */
    void close();

}
