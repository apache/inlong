/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.connector.source.enumerator;

import io.tidb.bigdata.flink.connector.source.split.TiDBSourceSplit;
import io.tidb.bigdata.flink.tidb.TiDBBaseDynamicTableFactory;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.SplitInternal;
import io.tidb.bigdata.tidb.SplitManagerInternal;
import io.tidb.bigdata.tidb.TableHandleInternal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

public class TiDBSourceSplitEnumerator implements
    SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceSplitEnumerator.class);

  private final Map<String, String> properties;
  private final SplitEnumeratorContext<TiDBSourceSplit> context;

  private final Map<Integer, Set<TiDBSourceSplit>> pendingSplitAssignment;
  private final Set<Integer> assignedReaders;
  private final Set<Integer> notifiedReaders;
  private final Set<TiDBSourceSplit> assignedSplits;
  private TiTimestamp timestamp;

  public TiDBSourceSplitEnumerator(
      Map<String, String> properties,
      SplitEnumeratorContext<TiDBSourceSplit> context) {
    this(properties, context, Collections.emptySet());
  }

  public TiDBSourceSplitEnumerator(
      Map<String, String> properties,
      SplitEnumeratorContext<TiDBSourceSplit> context,
      Set<TiDBSourceSplit> assignedSplits) {
    this.properties = properties;
    this.context = context;
    this.assignedSplits = new HashSet<>(assignedSplits);
    this.pendingSplitAssignment = new HashMap<>();
    this.assignedReaders = new HashSet<>();
    this.notifiedReaders = new HashSet<>();
    initPendingSplitAssignment();
  }

  private void assignPendingSplits(Set<Integer> pendingReaders) {
    Map<Integer, List<TiDBSourceSplit>> incrementalAssignment = new HashMap<>();

    // Check if there's any pending splits for given readers
    for (int pendingReader : pendingReaders) {
      // Remove pending assignment for the reader
      final Set<TiDBSourceSplit> pendingAssignmentForReader =
          pendingSplitAssignment.remove(pendingReader);

      if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
        // Put pending assignment into incremental assignment
        incrementalAssignment
            .computeIfAbsent(pendingReader, (key) -> new ArrayList<>())
            .addAll(pendingAssignmentForReader);

        // Make pending partitions as already assigned
        assignedSplits.addAll(pendingAssignmentForReader);
      }
      assignedReaders.add(pendingReader);
    }

    // Assign pending splits to readers
    if (!incrementalAssignment.isEmpty()) {
      LOG.info("Assigning splits to readers {}", incrementalAssignment);
      context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
    }

    for (int reader : assignedReaders) {
      if (notifiedReaders.contains(reader) || !context.registeredReaders().containsKey(reader)) {
        continue;
      }
      context.signalNoMoreSplits(reader);
      notifiedReaders.add(reader);
    }
  }

  public void initPendingSplitAssignment() {
    try (ClientSession splitSession = ClientSession.create(new ClientConfig(properties))) {
      // check exist
      final String databaseName = properties.get(TiDBBaseDynamicTableFactory.DATABASE_NAME.key());
      final String tableName = properties.get(TiDBBaseDynamicTableFactory.TABLE_NAME.key());
      splitSession.getTableMust(databaseName, tableName);
      timestamp = splitSession.getSnapshotVersion();
      final TableHandleInternal tableHandleInternal = new TableHandleInternal(
          UUID.randomUUID().toString(), databaseName, tableName);
      List<SplitInternal> splits =
          new SplitManagerInternal(splitSession).getSplits(tableHandleInternal, timestamp);
      List<TiDBSourceSplit> allSplits = splits.stream().map(TiDBSourceSplit::new)
          .collect(Collectors.toList());
      int parallelism = context.currentParallelism();
      for (int i = 0; i < allSplits.size(); i++) {
        int reader = i % parallelism;
        pendingSplitAssignment.computeIfAbsent(reader, integer -> new HashSet<>())
            .add(allSplits.get(i));
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void start() {
  }

  public TiTimestamp getTimestamp() {
    return timestamp;
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostName) {
  }

  @Override
  public void addSplitsBack(List<TiDBSourceSplit> splits, int subtaskId) {
    this.pendingSplitAssignment.computeIfAbsent(subtaskId, key -> new HashSet<>()).addAll(splits);
    this.notifiedReaders.remove(subtaskId);
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.debug("Adding reader {} to TiDBSourceSplitEnumerator", subtaskId);
    assignPendingSplits(Collections.singleton(subtaskId));
  }

  @Override
  public TiDBSourceSplitEnumState snapshotState(long l) {
    return new TiDBSourceSplitEnumState(assignedSplits);
  }

  @Override
  public void close() {
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent event) {
    SplitEnumerator.super.handleSourceEvent(subtaskId, event);
  }
}
