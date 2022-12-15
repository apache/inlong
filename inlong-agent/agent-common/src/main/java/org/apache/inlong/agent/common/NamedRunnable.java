package org.apache.inlong.agent.common;

/**
 * A runnable function with name.
 */
public interface NamedRunnable extends Runnable {
    String getName();
}
