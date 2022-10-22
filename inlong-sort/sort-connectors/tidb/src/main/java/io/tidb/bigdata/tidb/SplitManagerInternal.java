/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.tidb;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.meta.TiTimestamp;

public final class SplitManagerInternal {

  static final Logger LOG = LoggerFactory.getLogger(SplitManagerInternal.class);

  private final ClientSession session;

  public SplitManagerInternal(ClientSession session) {
    this.session = requireNonNull(session, "session is null");
  }

  public List<SplitInternal> getSplits(TableHandleInternal tableHandle) {
    return getSplits(tableHandle, session.getSnapshotVersion());
  }

  public List<SplitInternal> getSplits(TableHandleInternal tableHandle, TiTimestamp timestamp) {
    List<SplitInternal> splits = session.getTableRanges(tableHandle)
        .stream()
        .map(range -> new SplitInternal(tableHandle, range, timestamp))
        .collect(toCollection(ArrayList::new));
    Collections.shuffle(splits);
    LOG.info("The number of split for table `{}`.`{}` is {}", tableHandle.getSchemaName(),
        tableHandle.getTableName(), splits.size());
    return Collections.unmodifiableList(splits);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .toString();
  }
}