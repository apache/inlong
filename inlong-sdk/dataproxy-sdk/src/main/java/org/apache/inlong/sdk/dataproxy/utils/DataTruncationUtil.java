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

package org.apache.inlong.sdk.dataproxy.utils;

import org.apache.inlong.sdk.dataproxy.ConfigConstants;

import java.util.ArrayList;
import java.util.List;

public class DataTruncationUtil {

    /**
     * truncate data body
     * @param body
     * @return byte[]
     */
    public static byte[] truncateData(byte[] body) {
        byte[] newBody = new byte[ConfigConstants.MAX_MESSAGE_LENGTH];
        System.arraycopy(body, 0, newBody, 0, ConfigConstants.MAX_MESSAGE_LENGTH);
        return newBody;
    }

    /**
     * truncate data body list if it is too long
     * @param bodyList
     * @return List<byte[]>
     */
    public static List<byte[]> truncateData(List<byte[]> bodyList) {
        int size = 0;
        List<byte[]> newBodyList = new ArrayList<>();
        for (byte[] body : bodyList) {
            if (body.length + size <= ConfigConstants.MAX_MESSAGE_LENGTH) {
                newBodyList.add(body);
                size += body.length;
            } else {
                break;
            }
        }
        return newBodyList;
    }
}
