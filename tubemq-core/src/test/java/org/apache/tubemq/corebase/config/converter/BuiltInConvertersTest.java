/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.config.converter;

import org.apache.tubemq.corebase.config.ConfigurationException;
import org.apache.tubemq.corebase.config.converter.builtin.BooleanConverter;
import org.apache.tubemq.corebase.config.converter.builtin.ByteConverter;
import org.apache.tubemq.corebase.config.converter.builtin.DefaultConverter;
import org.apache.tubemq.corebase.config.converter.builtin.DoubleConverter;
import org.apache.tubemq.corebase.config.converter.builtin.FloatConverter;
import org.apache.tubemq.corebase.config.converter.builtin.IntegerConverter;
import org.apache.tubemq.corebase.config.converter.builtin.LongConverter;
import org.apache.tubemq.corebase.config.converter.builtin.StringConverter;
import org.junit.Assert;
import org.junit.Test;

public class BuiltInConvertersTest {

    @Test
    public void testBooleanConverter() {
        Converter<Object, Boolean> booleanConverter = BooleanConverter.getInstance();
        Assert.assertTrue(booleanConverter.doConvert("true"));
        Assert.assertFalse(booleanConverter.doConvert("false"));
        Assert.assertNull(booleanConverter.doConvert(null));
        Assert.assertFalse(booleanConverter.doConvert("q"));
    }

    @Test
    public void testLongConverter() {
        Converter<Object, Long> longConverter = LongConverter.getInstance();
        Assert.assertEquals(1L, (long) longConverter.doConvert("1"));
        Assert.assertNull(longConverter.doConvert(null));
        try {
            longConverter.doConvert("edd");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
    }

    @Test
    public void testStringConverter() {
        Converter<Object, String> stringConverter = StringConverter.getInstance();
        Assert.assertEquals(stringConverter.doConvert("1"), "1");
        Assert.assertNull(stringConverter.doConvert(null));
        Assert.assertEquals(stringConverter.doConvert(1), "1");
    }

    @Test
    public void testFloatConverter() {
        Converter<Object, Float> floatConverter = FloatConverter.getInstance();
        Assert.assertEquals(1.0, floatConverter.doConvert("1"), 0.0);
        Assert.assertEquals(1.2345f, floatConverter.doConvert("1.2345"), 0.0);
        Assert.assertNull(floatConverter.doConvert(null));
        try {
            floatConverter.doConvert("edd");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
    }

    @Test
    public void testDoubleConverter() {
        Converter<Object, Double> doubleConverter = DoubleConverter.getInstance();
        Assert.assertEquals(1.0, doubleConverter.doConvert("1"), 0.0);
        Assert.assertEquals(1.2345d, doubleConverter.doConvert("1.2345"), 0.0);
        Assert.assertNull(doubleConverter.doConvert(null));
        try {
            doubleConverter.doConvert("edd");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
    }

    @Test
    public void testByteConverter() {
        Converter<Object, Byte> byteConverter = ByteConverter.getInstance();
        Assert.assertEquals(1, byteConverter.doConvert("1"), 0.0);
        Assert.assertNull(byteConverter.doConvert(null));
        try {
            byteConverter.doConvert("edd");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
        try {
            Assert.assertEquals(129, byteConverter.doConvert("129"), 0.0);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
    }

    @Test
    public void testIntegerConverter() {
        Converter<Object, Integer> integerConverter = IntegerConverter.getInstance();
        Assert.assertEquals(1, integerConverter.doConvert("1"), 0.0);
        Assert.assertNull(integerConverter.doConvert(null));
        try {
            integerConverter.doConvert("edd");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
        try {
            Assert.assertEquals(123, integerConverter.doConvert(123.4), 0.0);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigurationException);
        }
    }

    @Test
    public void testDefaultConverter() {
        Converter<Object, Object> defaultConverter = DefaultConverter.getInstance();
        Assert.assertNull(defaultConverter.doConvert(null));
        Object object = new Object();
        Assert.assertEquals(defaultConverter.doConvert(object), object);
    }
}
