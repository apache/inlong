package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class TestEndsWithFunction extends AbstractFunctionStringTestBase {

    @Test
    public void testEndsWithFunction() throws Exception {
        String transformSql1 = "select endswith(encode(string1, string2), encode(string3, string2)) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case1: endswith(encode('Hello','UTF-8'), encode('lo','UTF-8'))
        List<String> output1 = processor1.transform("Hello|UTF-8|lo|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=true");

        // case2: endswith(encode('Hello','UTF-8'), encode('H','UTF-8'))
        List<String> output2 = processor1.transform("Hello|UTF-8|H|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=false");

        // case3: endswith(encode('Hello','UTF-8'), encode('LO','UTF-8'))
        List<String> output3 = processor1.transform("Hello|UTF-8|LO|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=false");

        // below are tests for String params; while upper are tests for byte[] params
        String transformSql2 = "select endswith(string1, stringX) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: endswith('Apache InLong', null)
        List<String> output4 = processor2.transform("Apache InLong|UTF-16BE|UTF-16BE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");

        String transformSql3 = "select endswith(stringX, string2) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: endswith(null, 'Apache InLong')
        List<String> output5 = processor2.transform("Apache InLong|Apache InLong|UTF-16BE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=");

        String transformSql4 = "select endswith(string1, string2) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case6: endswith('Apache InLong', 'Apache InLong')
        List<String> output6 = processor4.transform("Apache InLong|Apache InLong|UtF-16|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=true");

        // case7: endswith('Apache InLong', 'Apache')
        List<String> output7 = processor4.transform("Apache InLong|Apache|UTF-16--|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=false");

        // case8: endswith('Apache InLong', 'Long')
        List<String> output8 = processor4.transform("Apache InLong|Long|UTf-16LE|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=true");

        // case6: endswith('Apache InLong', '')
        List<String> output9 = processor4.transform("Apache InLong||UtF-16|cloud|1", new HashMap<>());
        Assert.assertEquals(1, output9.size());
        Assert.assertEquals(output9.get(0), "result=true");

    }
}
