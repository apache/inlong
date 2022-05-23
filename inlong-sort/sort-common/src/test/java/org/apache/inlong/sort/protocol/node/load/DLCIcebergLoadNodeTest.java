/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.inlong.sort.protocol.node.load;

import org.apache.inlong.sort.SerializeBaseTest;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DLCIcebergLoadNodeTest extends SerializeBaseTest<DLCIcebergLoadNode> {
    @Override
    public DLCIcebergLoadNode getTestObject() {
        Map<String, String> properties = new HashMap<>();
        properties.put("qcloud.dlc.secret-id", "XxxxxxxxXxxxxxxxxxxxxxxxxxxxXx");
        properties.put("qcloud.dlc.secret-key", "rQikVzC8oDLCn9E3btue7KGwLUM");
        properties.put("qcloud.dlc.region", "ap-beijing");

        properties.put("fs.cosn.bucket.region", "ap-beijing");
        properties.put("lake.fs.authentication.url", "http://9.164.178.124:2025/v1/internal/GetLakeFsAccessToken");
        properties.put("lake.fs.user.appid", "1231122322");
        return new DLCIcebergLoadNode(
                "iceberg_dlc",
                "iceberg_dlc_output",
                Arrays.asList(new FieldInfo("field", new StringFormatInfo())),
                Arrays.asList(new FieldRelationShip(new FieldInfo("field", new StringFormatInfo()),
                        new FieldInfo("field", new StringFormatInfo()))),
                null,
                null,
                null,
                properties,
                "inlong",
                "inlong_iceberg_dlc",
                null,
                null,
                "cos://localhost:9000/iceberg_dlc/warehouse");
    }
}
