/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.broker.msgstore.disk;

import com.carrotsearch.hppc.ObjectArrayDeque;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * FileSegments management. Contains two types FileSegment: data and index.
 */
public class FileSegmentList implements SegmentList {
    private static final Logger logger =
            LoggerFactory.getLogger(FileSegmentList.class);
    private AtomicBoolean preemptiveOwnership = new AtomicBoolean(false);
    private ObjectArrayDeque<Segment> segmentList2 = new ObjectArrayDeque<Segment>(10000);

    public FileSegmentList(final Segment[] s) {
        super();
        for (Segment e : s) {
            segmentList2.addLast(e);
        }
    }

    public FileSegmentList() {
        super();
    }

    @Override
    public void close() {
        // There's "forEach-Lambda" sugar, but it's for Java 1.8+
        Iterator<ObjectCursor<Segment>> it = getIterator(true);
        for (ObjectCursor<Segment> s = it.next(); it.hasNext(); it.next()) {
            Segment segment = s.value;
            if (segment != null) {
                segment.close();
            }
        }
    }

    @Override
    /**
     * Here it returns a *snapshot* of current list, so generally it's NOT encouraged
     * to call this function, unless there's special need to take it.
     * Please use getIterator() instead.
     * @see getIterator
     */
    public Segment[] getView() {
        return segmentList2.toArray(new Segment[segmentList2.size()]);
    }

    /**
     * As for performance consideration, it's complex but efficient.
     * @param isAscent
     * @return
     * @see FileSegmentList::getView
     */
    public Iterator<ObjectCursor<Segment>> getIterator(boolean isAscent) {
        return (isAscent) ? segmentList2.iterator() : segmentList2.descendingIterator();
    }

    /***
     * Return segment by the given offset.
     *
     * @param offset
     * @return
     * @throws IOException
     */
    @Override
    public Segment getRecordSeg(final long offset) throws IOException {
        Segment tmpSeg = this.findSegment(offset);
        if (tmpSeg.isExpired()) {
            return null;
        }
        return tmpSeg;
    }

    @Override
    public void append(final Segment segment) {
        while (!preemptiveOwnership.weakCompareAndSet(false, true)) {
            // implicit yield by while-weakCompareAndSet.
        }
        segmentList2.addLast(segment);
        preemptiveOwnership.set(false);
    }

    /***
     * Check each FileSegment whether is expired, and set expire status.
     *
     * @param checkTimestamp
     * @param fileValidTimeMs
     * @return
     */
    @Override
    public boolean checkExpiredSegments(final long checkTimestamp, final long fileValidTimeMs) {
        boolean hasExpired = false;
        Iterator<ObjectCursor<Segment>> it = getIterator(true);
        for (ObjectCursor<Segment> s = it.next(); it.hasNext(); it.next()) {
            Segment segment = s.value;
            if (segment == null) {
                continue;
            }
            if (segment.checkAndSetExpired(checkTimestamp, fileValidTimeMs) == 0) {
                break;
            }
            hasExpired = true;
        }
        return hasExpired;
    }

    /***
     * Check FileSegments whether is expired, close all expired FileSegments, and then delete these files.
     *
     * @param sb
     */
    @Override
    public void delExpiredSegments(final StringBuilder sb) {
        //　delete expired segment
        Iterator<ObjectCursor<Segment>> it = getIterator(true);
        for (ObjectCursor<Segment> s = it.next(); it.hasNext(); it.next()) {
            Segment segment = s.value;
            if (segment == null) {
                continue;
            }
            if (!segment.needDelete()) {
                break;
            }
            delete(segment);
            segment.deleteFile();
        }
    }

    @Override
    public void delete(final Segment segment) {
        while (!preemptiveOwnership.weakCompareAndSet(false, true)) {
            // implicit yield by while-weakCompareAndSet.
        }
        segmentList2.removeFirst(segment);
        preemptiveOwnership.set(false);
    }

    @Override
    public void flushLast(boolean force) throws IOException {
        Segment segment = segmentList2.getLast();
        if (segment != null && !segment.isExpired()) {
            segment.flush(force);
        }
    }

    @Override
    public Segment last() {
        return segmentList2.getLast();
    }

    /***
     * Return the start position of these FileSegments.
     *
     * @return
     */
    @Override
    public long getMinOffset() {
        long last = 0L;
        Iterator<ObjectCursor<Segment>> it = getIterator(true);
        for (ObjectCursor<Segment> s = it.next(); it.hasNext(); it.next()) {
            Segment segment = s.value;
            if (segment == null) {
                continue;
            }

            if (segment.isExpired()) {
                last = segment.getCommitLast();
                continue;
            } else {
                last = segment.getStart();
                break;
            }
        }
        return last;
    }

    /***
     * Return the max position of these FileSegments.
     *
     * @return
     */
    @Override
    public long getMaxOffset() {
        Segment lastSegment = segmentList2.getLast();
        if (lastSegment != null) {
            return lastSegment.getLast();
        } else {
            return 0L;
        }
    }

    /***
     * Return the max position that have been flushed to disk.
     *
     * @return
     */
    @Override
    public long getCommitMaxOffset() {
        Segment lastSegment = segmentList2.getLast();
        if (lastSegment != null) {
            return lastSegment.getCommitLast();
        } else {
            return 0L;
        }
    }

    @Override
    public long getSizeInBytes() {
        long sum = 0L;
        Iterator<ObjectCursor<Segment>> it = getIterator(false);
        for (ObjectCursor<Segment> s = it.next(); it.hasNext(); it.next()) {
            Segment segment = s.value;
            if (segment == null) {
                continue;
            }
            if (segment.isExpired()) {
                break;
            }
            sum += segment.getCachedSize();
        }
        return sum;
    }

    /**
     * Internal Usage ONLY: get the binary middle index to segmentList2, for binary search.
     * @param lowerBound
     * @param upperBound
     * @return
     */
    private int getMidIndex(int lowerBound, int upperBound) {
        int range = segmentList2.buffer.length;
        int upperFill = (lowerBound > upperBound) ? range : 0;
        return (lowerBound + upperBound + upperFill) >>> 1;
    }

    /**
     *  Binary search the segment that contains the offset
     * @param offset
     * @return
     */
    @Override
    public Segment findSegment(final long offset) {
        int lowerBound = segmentList2.head;
        int upperBound = segmentList2.tail;
        Object[] internalList = segmentList2.buffer;
        int range = segmentList2.buffer.length;

        // Re-locate not-expired lowerBound
        for (int walk = lowerBound; walk <= upperBound; walk = (walk + 1) % range) {
            Segment walkSegment = (Segment) internalList[walk];
            if (walkSegment != null && !walkSegment.isExpired()) {
                lowerBound = walk;
                break;
            }
        }
        // Under-run check
        Segment lowerSegment = (Segment) internalList[lowerBound];
        if (offset < lowerSegment.getStart()) {
            throw new ArrayIndexOutOfBoundsException(new StringBuilder(512)
                    .append("Request offsets is ").append(offset)
                    .append(", the start is ").append(lowerSegment.getStart()).toString());
        }
        // Overflow check
        Segment upperSegment = (Segment) internalList[lowerBound];
        if (offset > upperSegment.getLast()) {
            throw new ArrayIndexOutOfBoundsException(new StringBuilder(512)
                    .append("Request offsets is ").append(offset)
                    .append(", the boundary is ").append(upperSegment.getLast()).toString());
        }

        // Binary search
        while (lowerBound != upperBound) {
            int middleIndex = getMidIndex(lowerBound, upperBound);
            Segment middleSegment = (Segment) internalList[middleIndex];
            if (offset < middleSegment.getStart()) {
                upperBound = middleIndex;
                continue;
            } else if (offset > middleSegment.getLast()) {
                lowerBound = middleIndex;
                continue;
            }
            return middleSegment;
        }

        // Fail-safe: In case of offset-gap between segments.
        // Usually it means a bug when it returns null instead of throwing an exception.
        return null;
    }

}
