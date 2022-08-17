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

package org.apache.inlong.sort.formats.common;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Collections;
import org.junit.Test;

/**
 * Unit tests for {@link FunctionFormatInfo}.
 */
public class FunctionFormatInfoTest extends FormatInfoTestBase {

    @Override
    Collection<FormatInfo> createFormatInfos() {
        return Collections.singletonList(FunctionFormatInfo.INSTANCE);
    }

    @Test
    public void testFunctionFormat() {
        FieldInfo stringFiled = new FieldInfo("123", new StringFormatInfo());
        assertEquals("`123`", stringFiled.format());

        FieldInfo functionFiled = new FieldInfo("ABS(`num`)", new FunctionFormatInfo());
        assertEquals("ABS(`num`)", functionFiled.format());
    }

    public class FieldInfo {

        private String name;
        private FormatInfo formatInfo;

        public FieldInfo(String name, FormatInfo formatInfo) {
            this.name = name;
            this.formatInfo = formatInfo;
        }

        public FieldInfo(String name) {
            this(name, null);
        }

        public String format() {
            String formatName = name.trim();
            if (!formatName.startsWith("`") && !(formatInfo instanceof FunctionFormatInfo)) {
                formatName = String.format("`%s", formatName);
            }
            if (!formatName.endsWith("`") && !(formatInfo instanceof FunctionFormatInfo)) {
                formatName = String.format("%s`", formatName);
            }
            return formatName;
        }
    }
}
