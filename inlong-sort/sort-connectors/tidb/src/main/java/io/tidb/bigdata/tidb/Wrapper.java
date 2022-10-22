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

import java.util.Objects;

public class Wrapper<T> {

  private final T internal;

  protected Wrapper(T internal) {
    this.internal = internal;
  }

  public T getInternal() {
    return internal;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    Wrapper<T> other = (Wrapper<T>) obj;
    return Objects.equals(this.internal, other.internal);
  }

  @Override
  public int hashCode() {
    return internal.hashCode();
  }

  @Override
  public String toString() {
    return internal.toString();
  }
}