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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

final class Serialization {

  static <T extends Serializable> String serialize(T o) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.close();
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  static <T extends Serializable> T deserialize(String base64) {
    try {
      ObjectInputStream ois = new ObjectInputStream(
          new ByteArrayInputStream(Base64.getDecoder().decode(base64)));
      return (T) ois.readObject();
    } catch (IOException | ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }
}