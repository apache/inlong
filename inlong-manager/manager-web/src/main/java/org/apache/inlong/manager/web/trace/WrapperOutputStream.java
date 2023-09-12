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

package org.apache.inlong.manager.web.trace;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.CoyoteOutputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper output stream
 * When writing character data, write the data into the output stream of OutputStream and HttpServletResponse object at the same time.
 */
@Slf4j
public class WrapperOutputStream extends ServletOutputStream {

    private OutputStream innerOut;
    private HttpServletResponse response;

    public WrapperOutputStream(OutputStream innerOut, HttpServletResponse response) {
        super();
        this.response = response;
        this.innerOut = innerOut;
    }

    @Override
    public boolean isReady() {
        if (response == null) {
            return false;
        }
        try {
            return response.getOutputStream().isReady();
        } catch (IOException e) {
            log.error("got exception when check if  the output stream is ready", e);
        }
        return false;
    }

    @Override
    public void setWriteListener(WriteListener listener) {
        if (response != null) {
            try {
                ((CoyoteOutputStream) response.getOutputStream()).setWriteListener(listener);
            } catch (IOException e) {
                log.error("got exception when set write listener", e);
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (response != null) {
            response.getOutputStream().write(b);
        }
        innerOut.write(b);
    }
}
