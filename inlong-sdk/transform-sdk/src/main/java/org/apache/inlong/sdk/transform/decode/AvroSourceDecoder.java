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
import org.apache.inlong.sdk.transform.process.Context;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class AvroSourceDecoder extends SourceDecoder<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSourceDecoder.class);

    protected AvroSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private String rowsNodePath;
    private List<AvroNode> childNodes;

    public AvroSourceDecoder(AvroSourceInfo sourceInfo) {
        try {
            this.sourceInfo = sourceInfo;
            if (!StringUtils.isBlank(sourceInfo.getCharset())) {
                this.srcCharset = Charset.forName(sourceInfo.getCharset());
            }
            this.rowsNodePath = sourceInfo.getRowsNodePath();
            if (!StringUtils.isBlank(rowsNodePath)) {
                this.childNodes = new ArrayList<>();
                String[] nodeStrings = this.rowsNodePath.split("\\.");
                for (String nodeString : nodeStrings) {
                    this.childNodes.add(new AvroNode(nodeString));
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new TransformException(e.getMessage(), e);
        }
    }

    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        try {
            InputStream inputStream = new ByteArrayInputStream(srcBytes);
            DataFileStream<GenericRecord> dataFileStream =
                    new DataFileStream<>(inputStream, new GenericDatumReader<>());
            GenericRecord root = dataFileStream.next();
            List<GenericRecord> childRoot = null;
            if (CollectionUtils.isEmpty(childNodes)) {
                return new AvroSourceData(root, null, srcCharset, context);
            }

            Object current = root;
            Schema curSchema = root.getSchema();

            for (AvroNode node : childNodes) {
                if (curSchema.getType() != Type.RECORD) {
                    // error data
                    return new AvroSourceData(root, null, srcCharset, context);
                }
                Object newElement = ((GenericRecord) current).get(node.getName());
                if (newElement == null) {
                    // error data
                    return new AvroSourceData(root, null, srcCharset, context);
                }
                // node is not array
                if (!node.isArray()) {
                    curSchema = curSchema.getField(node.getName()).schema();
                    current = newElement;
                    continue;
                }
                // node is an array
                current = getElementFromArray(node, newElement, curSchema);
                if (current == null) {
                    // error data
                    return new AvroSourceData(root, null, srcCharset, context);
                }
            }
            if (curSchema.getType() != Type.ARRAY) {
                // error data
                return new AvroSourceData(root, null, srcCharset, context);
            }
            childRoot = (List<GenericRecord>) current;
            return new AvroSourceData(root, childRoot, srcCharset, context);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    private Object getElementFromArray(AvroNode node, Object curElement, Schema curSchema) {
        if (node.getArrayIndices().isEmpty()) {
            // error data
            return null;
        }
        for (int index : node.getArrayIndices()) {
            if (curSchema.getType() != Type.ARRAY) {
                // error data
                return null;
            }
            List<?> newArray = (List<?>) curElement;
            if (index >= newArray.size()) {
                // error data
                return null;
            }
            curSchema = curSchema.getElementType();
            curElement = newArray.get(index);
        }
        return curElement;
    }

}
