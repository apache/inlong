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

package org.apache.inlong.sort.singletenant.flink.utils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.inlong.sort.formats.common.ArrayTypeInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanTypeInfo;
import org.apache.inlong.sort.formats.common.MapTypeInfo;
import org.apache.inlong.sort.formats.common.RowTypeInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.StringTypeInfo;
import org.apache.inlong.sort.formats.common.TypeInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.junit.Test;

import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.convertFieldInfosToRowTypeInfo;
import static org.apache.inlong.sort.singletenant.flink.utils.CommonUtils.getTypeInformationFromTypeInfo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommonUtilsTest {

    @Test
    public void testGetTypeInformationFromTypeInfo() {
        // simple type
        TypeInformation<?> booleanTypeInformation = getTypeInformationFromTypeInfo(new BooleanTypeInfo());
        assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, booleanTypeInformation);

        // Array type
        ArrayTypeInfo arrayTypeInfoSort = new ArrayTypeInfo(new ArrayTypeInfo(new StringTypeInfo()));
        TypeInformation<?> arrayTypeInformation = getTypeInformationFromTypeInfo(arrayTypeInfoSort);
        assertTrue(arrayTypeInformation instanceof ObjectArrayTypeInfo);
        ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) arrayTypeInformation;
        TypeInformation componentInfo = objectArrayTypeInfo.getComponentInfo();
        assertTrue(componentInfo instanceof ObjectArrayTypeInfo);
        ObjectArrayTypeInfo innerObjectArrayTypeInfo = (ObjectArrayTypeInfo) componentInfo;
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, innerObjectArrayTypeInfo.getComponentInfo());

        // Map type
        MapTypeInfo mapTypeInfoSort =
                new MapTypeInfo(new StringTypeInfo(), new MapTypeInfo(new StringTypeInfo(), arrayTypeInfoSort));
        TypeInformation<?> mapTypeInformation = getTypeInformationFromTypeInfo(mapTypeInfoSort);
        assertTrue(mapTypeInformation instanceof org.apache.flink.api.java.typeutils.MapTypeInfo);
        org.apache.flink.api.java.typeutils.MapTypeInfo mapTypeInfoFlink =
                (org.apache.flink.api.java.typeutils.MapTypeInfo) mapTypeInformation;
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, mapTypeInfoFlink.getKeyTypeInfo());
        TypeInformation valueTypeInfo = mapTypeInfoFlink.getValueTypeInfo();
        assertTrue(valueTypeInfo instanceof org.apache.flink.api.java.typeutils.MapTypeInfo);
        org.apache.flink.api.java.typeutils.MapTypeInfo valueTypeInfoFlink =
                (org.apache.flink.api.java.typeutils.MapTypeInfo) valueTypeInfo;
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, valueTypeInfoFlink.getKeyTypeInfo());
        assertEquals(arrayTypeInformation, valueTypeInfoFlink.getValueTypeInfo());

        // row type
        RowTypeInfo rowTypeInfoSort = new RowTypeInfo(
                new String[]{"field1", "field2", "field3"},
                new TypeInfo[]{
                        arrayTypeInfoSort,
                        mapTypeInfoSort,
                        new RowTypeInfo(new String[]{"field31"}, new TypeInfo[]{new StringTypeInfo()})
                }
        );
        TypeInformation<?> rowTypeInformation = getTypeInformationFromTypeInfo(rowTypeInfoSort);
        assertTrue(rowTypeInformation instanceof org.apache.flink.api.java.typeutils.RowTypeInfo);
        org.apache.flink.api.java.typeutils.RowTypeInfo rowTypeInfoFlink =
                (org.apache.flink.api.java.typeutils.RowTypeInfo) rowTypeInformation;
        assertEquals(3, rowTypeInfoFlink.getArity());
        assertArrayEquals(new String[]{"field1", "field2", "field3"}, rowTypeInfoFlink.getFieldNames());
        TypeInformation<?>[] fieldTypes = rowTypeInfoFlink.getFieldTypes();
        assertEquals(fieldTypes[0], arrayTypeInformation);
        assertEquals(fieldTypes[1], mapTypeInformation);
        assertTrue(fieldTypes[2] instanceof org.apache.flink.api.java.typeutils.RowTypeInfo);
        org.apache.flink.api.java.typeutils.RowTypeInfo innerRowTypeInfoFlink =
                (org.apache.flink.api.java.typeutils.RowTypeInfo) fieldTypes[2];
        assertArrayEquals(new String[]{"field31"}, innerRowTypeInfoFlink.getFieldNames());
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, innerRowTypeInfoFlink.getFieldTypes()[0]);
    }

    @Test
    public void testConvertFieldInfosToRowTypeInfo() {
        org.apache.flink.api.java.typeutils.RowTypeInfo rowTypeInfoFlink =
                convertFieldInfosToRowTypeInfo(new FieldInfo[]{
                        new FieldInfo("field1", new StringFormatInfo()),
                        new FieldInfo("field2", new BooleanFormatInfo())
                });
        assertArrayEquals(new String[]{"field1", "field2"}, rowTypeInfoFlink.getFieldNames());
        TypeInformation<?>[] fieldTypesFlink = rowTypeInfoFlink.getFieldTypes();
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, fieldTypesFlink[0]);
        assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, fieldTypesFlink[1]);
    }
}
