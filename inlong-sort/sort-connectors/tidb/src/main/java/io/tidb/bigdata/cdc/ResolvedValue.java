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

package io.tidb.bigdata.cdc;

import io.tidb.bigdata.cdc.json.jackson.JacksonFactory;
import java.util.Optional;

/*
 * TiCDC resolved event value
 */
public final class ResolvedValue implements Value {

  private static final ResolvedValue INSTANCE = new ResolvedValue();

  private ResolvedValue() {
  }

  public static ResolvedValue getInstance() {
    return INSTANCE;
  }

  @Override
  public Optional<RowChangedValue> asRowChanged() {
    return Optional.empty();
  }

  @Override
  public Optional<ResolvedValue> asResolved() {
    return Optional.of(this);
  }

  @Override
  public Optional<DDLValue> asDDL() {
    return Optional.empty();
  }

  @Override
  public String toJson(JacksonFactory factory) {
    return null;
  }
}
