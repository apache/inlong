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

import org.apache.inlong.sdk.transform.pojo.ParquetSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * PbSourceDecoder
 */
public class ParquetSourceDecoder implements SourceDecoder<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetSourceDecoder.class);

    protected ParquetSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private MessageType schema;
    private String childMessagePath;
    private String rootMessageLabel;

    public ParquetSourceDecoder(ParquetSourceInfo sourceInfo) {
        try {
            this.sourceInfo = sourceInfo;
            if (!StringUtils.isBlank(sourceInfo.getCharset())) {
                this.srcCharset = Charset.forName(sourceInfo.getCharset());
            }
            this.schema = MessageTypeParser.parseMessageType(sourceInfo.getParquetSchema());
            this.childMessagePath = sourceInfo.getChildMessagePath();
            this.rootMessageLabel = sourceInfo.getRootMessageLabel();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new TransformException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        try {
            // Create a custom InputFile
            InputFile inputFile = new ParquetInputByteArray(srcBytes);

            // Read Parquet data using ParquetFileReader
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                // Retrieve the metadata of the file
                ParquetMetadata footer = reader.getFooter();
                MessageType schema = footer.getFileMetaData().getSchema();

                PageReadStore pages;
                while ((pages = reader.readNextRowGroup()) != null) {
                    long rows = pages.getRowCount();

                    ColumnIOFactory factory = new ColumnIOFactory();
                    MessageColumnIO columnIO = factory.getColumnIO(schema);

                    RecordMaterializer<Group> recordMaterializer = new GroupRecordConverter(schema);
                    RecordReader<Group> recordReader = columnIO.getRecordReader(pages, recordMaterializer);

                    for (int i = 0; i < rows; i++) {
                        Group group = recordReader.read();
                        if (group != null) {
                            return new ParquetSourceData(group, this.childMessagePath, this.srcCharset);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }
}
