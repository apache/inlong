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

package io.tidb.bigdata.flink.connector.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

public class CDCSource<SplitT extends SourceSplit, EnumChkT>
        implements Source<RowData, SplitT, EnumChkT> {

    private final Source<RowData, SplitT, EnumChkT> wrapped;

    protected CDCSource(Source<RowData, SplitT, EnumChkT> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, SplitT> createReader(SourceReaderContext context)
            throws Exception {

        return wrapped.createReader(context);
    }

    @Override
    public SplitEnumerator<SplitT, EnumChkT> createEnumerator(SplitEnumeratorContext<SplitT> context)
            throws Exception {
        return wrapped.createEnumerator(context);
    }

    @Override
    public SplitEnumerator<SplitT, EnumChkT>
    restoreEnumerator(SplitEnumeratorContext<SplitT> context, EnumChkT o) throws Exception {
        return wrapped.restoreEnumerator(context, o);
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return wrapped.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {
        return wrapped.getEnumeratorCheckpointSerializer();
    }
}
