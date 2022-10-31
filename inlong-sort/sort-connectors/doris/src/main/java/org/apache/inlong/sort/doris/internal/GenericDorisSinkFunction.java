package org.apache.inlong.sort.doris.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.doris.table.DorisDynamicSchemaOutputFormat;

import javax.annotation.Nonnull;
import java.io.IOException;

@Internal
public class GenericDorisSinkFunction<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {

    private final DorisDynamicSchemaOutputFormat<T> outputFormat;

    public GenericDorisSinkFunction(@Nonnull DorisDynamicSchemaOutputFormat<T> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        outputFormat.setRuntimeContext(getRuntimeContext());
        outputFormat.initializeState(context);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
        outputFormat.snapshotState(context);
    }

    @Override
    public void close() throws IOException {
        outputFormat.close();
    }
}
