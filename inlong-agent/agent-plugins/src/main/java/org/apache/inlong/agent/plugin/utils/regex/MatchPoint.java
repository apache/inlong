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

package org.apache.inlong.agent.plugin.utils.regex;

public class MatchPoint {

    String str;
    int start;
    int end;

    MatchPoint(String str1, int start1, int end1) {
        this.str = str1;
        this.start = start1;
        this.end = end1;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str1) {
        str = str1;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start1) {
        start = start1;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end1) {
        end = end1;
    }
}
