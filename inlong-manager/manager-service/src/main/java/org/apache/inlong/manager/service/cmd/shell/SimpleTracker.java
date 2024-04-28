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

package org.apache.inlong.manager.service.cmd.shell;

import java.util.ArrayList;
import java.util.List;

public class SimpleTracker implements ShellTracker {

    private List<String> result = new ArrayList<String>();
    private int code;

    public int getCode() {
        return code;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#fail()
     */
    public void fail(int code) {
        this.code = code;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#lineChange(java.lang.String)
     */
    public void lineChange(String line) {
        result.add(line);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#setProcessId(long)
     */
    public void setProcessId(long arg0) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#setRunInfo(java.lang.String)
     */
    public void setRunInfo(String arg0) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#beforeStart()
     */
    public void beforeStart() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#start()
     */
    public void start() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yy.hiido.ShellTracker#success()
     */
    public void success() {
    }

    /**
     * @return the result
     */
    public List<String> getResult() {
        return result;
    }

}
