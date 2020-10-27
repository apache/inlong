package org.apache.tubemq.manager.entry;

public enum TopicStatus {

    ADDING(0), SUCCESS(1), FAILED(2), RETRY(3);

    private int value = 0;

    private TopicStatus(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }
}
