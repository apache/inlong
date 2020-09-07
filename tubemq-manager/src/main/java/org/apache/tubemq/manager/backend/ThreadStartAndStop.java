package org.apache.tubemq.manager.backend;

/**
 * Interface for starting and stopping backend threads.
 */
public interface ThreadStartAndStop {
    /**
     * start all threads.
     */
    void startThreads() throws Exception;

    /**
     * stop all threads.
     */
    void stopThreads() throws Exception;

    /**
     * wait for all thread finishing.
     */
    void join() throws Exception;
}
