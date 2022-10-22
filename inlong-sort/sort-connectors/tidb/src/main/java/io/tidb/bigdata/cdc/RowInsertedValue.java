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
import io.tidb.bigdata.cdc.json.jackson.JacksonObjectNode;
import java.util.Optional;

/*
 * TiCDC row inserted event value
 */
public final class RowInsertedValue extends RowChangedValue {

  public RowInsertedValue(final RowColumn[] newValue) {
    super(null, newValue);
  }

  public Type getType() {
    return Type.INSERT;
  }

  @Override
  public Optional<RowDeletedValue> asDeleted() {
    return Optional.empty();
  }

  @Override
  public Optional<RowUpdatedValue> asUpdated() {
    return Optional.empty();
  }

  @Override
  public Optional<RowInsertedValue> asInserted() {
    return Optional.of(this);
  }

  @Override
  public String toJson(JacksonFactory factory) {
    JacksonObjectNode node = factory.createObject();
    toJson(getNewValue(), node.putObject("u"));
    return factory.toJson(node);
  }
}
