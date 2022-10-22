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

package io.tidb.bigdata.flink.connector.source.split;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class TiDBSourceSplitSerializer implements SimpleVersionedSerializer<TiDBSourceSplit> {

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public byte[] serialize(TiDBSourceSplit split) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      split.serialize(dos);
      dos.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public TiDBSourceSplit deserialize(int version, byte[] bytes) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream dis = new DataInputStream(bais)) {
      return TiDBSourceSplit.deserialize(dis);
    }
  }
}
