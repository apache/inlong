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

import org.apache.inlong.sdk.transform.pojo.BsonSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.math.BigDecimal;

/**
 * BsonSourceDecoder
 */
@Slf4j
public class BsonSourceDecoder implements SourceDecoder<byte[]> {

    private final JsonSourceDecoder decoder;

    public BsonSourceDecoder(BsonSourceInfo sourceInfo) {
        decoder = new JsonSourceDecoder(sourceInfo);
    }

    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        return decoder.decode(parse(srcBytes), context);
    }

    public String parse(byte[] bsonData) {
        try {
            RawBsonDocument rawBsonDocument = new RawBsonDocument(bsonData);
            JsonWriterSettings writerSettings = JsonWriterSettings.builder()
                    .outputMode(JsonMode.RELAXED)
                    .decimal128Converter((value, writer) -> writer.writeNumber(value.bigDecimalValue().toPlainString()))
                    .doubleConverter((value, writer) -> writer.writeNumber(new BigDecimal(value).toString()))
                    .build();
            BsonDocument bsonDocument = BsonDocument.parse(rawBsonDocument.toJson(writerSettings));
            bsonDocument.remove("_id");
            return bsonDocument.toJson();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }
}
