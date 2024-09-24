/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.transform.decode;

import org.apache.inlong.sdk.transform.pojo.AvroSourceInfo;
import org.apache.inlong.sdk.transform.pojo.BsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.ParquetSourceInfo;
import org.apache.inlong.sdk.transform.pojo.PbSourceInfo;
import org.apache.inlong.sdk.transform.pojo.YamlSourceInfo;

public class SourceDecoderFactory {

    public static CsvSourceDecoder createCsvDecoder(CsvSourceInfo sourceInfo) {
        return new CsvSourceDecoder(sourceInfo);
    }

    public static KvSourceDecoder createKvDecoder(KvSourceInfo sourceInfo) {
        return new KvSourceDecoder(sourceInfo);
    }

    public static JsonSourceDecoder createJsonDecoder(JsonSourceInfo sourceInfo) {
        return new JsonSourceDecoder(sourceInfo);
    }

    public static PbSourceDecoder createPbDecoder(PbSourceInfo sourceInfo) {
        return new PbSourceDecoder(sourceInfo);
    }

    public static AvroSourceDecoder createAvroDecoder(AvroSourceInfo sourceInfo) {
        return new AvroSourceDecoder(sourceInfo);
    }

    public static BsonSourceDecoder createBsonDecoder(BsonSourceInfo sourceInfo) {
        return new BsonSourceDecoder(sourceInfo);
    }

    public static ParquetSourceDecoder createParquetDecoder(ParquetSourceInfo sourceInfo) {
        return new ParquetSourceDecoder(sourceInfo);
    }

    public static YamlSourceDecoder createYamlDecoder(YamlSourceInfo sourceInfo) {
        return new YamlSourceDecoder(sourceInfo);
    }

}
