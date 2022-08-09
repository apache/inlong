package org.apache.inlong.sdk.sort.api;

import org.apache.inlong.sdk.sort.entity.InLongTopic;

/**
 * the seeker is used to reset the offset of topic-partition of rollback task
 */
public interface Seeker {

    /**
     * configure seeker if topic properties have changed.
     * @param inLongTopic InlongTopic info.
     */
    void configure(InLongTopic inLongTopic);

    /**
     * do seek
     */
    void seek();

    /**
     * return the expected seek time of seeker
     * @return expected seek time
     */
    long getSeekTime();

}
