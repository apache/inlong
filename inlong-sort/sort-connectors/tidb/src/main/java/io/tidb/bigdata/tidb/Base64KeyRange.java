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

import java.util.Objects;

public final class Base64KeyRange {

  private String startKey;
  private String endKey;

  public Base64KeyRange(String startKey, String endKey) {
    this.startKey = requireNonNull(startKey, "startKey is null");
    this.endKey = requireNonNull(endKey, "endKey is null");
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startKey, endKey);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    Base64KeyRange other = (Base64KeyRange) obj;
    return Objects.equals(this.startKey, other.startKey) && Objects
        .equals(this.endKey, other.endKey);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("startKey", startKey)
        .add("endKey", endKey)
        .toString();
  }
}
