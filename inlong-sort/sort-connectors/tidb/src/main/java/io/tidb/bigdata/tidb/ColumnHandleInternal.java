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

import java.io.Serializable;
import java.util.Objects;
import org.tikv.common.types.DataType;

public final class ColumnHandleInternal implements Serializable {

  private final String name;
  private final DataType type;
  private final int ordinalPosition;

  public ColumnHandleInternal(String name, DataType type, int ordinalPosition) {
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
    this.ordinalPosition = ordinalPosition;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, ordinalPosition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    ColumnHandleInternal other = (ColumnHandleInternal) obj;
    return Objects.equals(this.name, other.name)
        && Objects.equals(this.type, other.type)
        && Objects.equals(this.ordinalPosition, other.ordinalPosition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("ordinalPosition", ordinalPosition)
        .toString();
  }
}