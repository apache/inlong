/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.hive.formats.orc;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.WriterImpl;

/**
 * Copy from org.apache.flink:flink-orc_2.11:1.13.1
 *
 * A factory that creates an ORC {@link BulkWriter}. The factory takes a {@link RowVectorizer} to convert the element
 * into an {@link org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch}.
 */
public class OrcBulkWriterFactory implements BulkWriter.Factory<Row> {

    private static final long serialVersionUID = 1429185895123369360L;

    private final RowVectorizer vectorizer;
    private final Properties writerProperties;
    private final int batchSize;

    private OrcFile.WriterOptions writerOptions;

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration, ORC writer properties.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a VectorizerRowBatch.
     * @param writerProperties Properties that can be used in ORC WriterOptions.
     */
    public OrcBulkWriterFactory(RowVectorizer vectorizer, Properties writerProperties, int batchSize) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writerProperties = writerProperties;
        this.batchSize = batchSize;
    }

    @Override
    public BulkWriter<Row> create(FSDataOutputStream out) throws IOException {
        OrcFile.WriterOptions opts = getWriterOptions();
        opts.physicalWriter(new PhysicalWriterImpl(out, opts));

        // The path of the Writer is not used to indicate the destination file in this case since we have used a
        // dedicated physical writer to write to the give output stream directly. However, the path would be used as
        // the key of writer in the ORC memory manager, thus we need to make it unique.
        Path unusedPath = new Path(UUID.randomUUID().toString());
        return new OrcBulkWriter(vectorizer, new WriterImpl(null, unusedPath, opts), batchSize);
    }

    private OrcFile.WriterOptions getWriterOptions() {
        if (null == writerOptions) {
            writerOptions = OrcFile
                    .writerOptions(writerProperties, new org.apache.hadoop.conf.Configuration())
                    .setSchema(this.vectorizer.getSchema());
        }

        return writerOptions;
    }

    public static BulkWriter.Factory<Row> createWriterFactory(
            RowType rowType,
            LogicalType[] fieldTypes,
            Configuration etlConf
    ) {
        final TypeDescription typeDescription = OrcBulkWriterUtil.logicalTypeToOrcType(rowType);
        final RowVectorizer rowVectorizer = new RowVectorizer(typeDescription, fieldTypes);

        return new OrcBulkWriterFactory(
                rowVectorizer,
                createWriterProps(etlConf),
                etlConf.getInteger(Constants.ORC_SINK_BATCH_SIZE)
        );
    }

    private static Properties createWriterProps(Configuration etlConf) {
        Properties writerProps = new Properties();
        etlConf.toMap().forEach((k, v) -> {
            if (k.startsWith(Constants.HIVE_SINK_ORC_PREFIX)) {
                writerProps.setProperty(k.substring(Constants.HIVE_SINK_PREFIX.length()), v);
            }
        });

        return writerProps;
    }
}
