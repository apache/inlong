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

import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Wrapper Writer
 * When writing character data, write the data into the output stream of ByteArrayOutputStream and HttpServletResponse object at the same time.
 */
public class WrapperWriter extends PrintWriter {

    private HttpServletResponse response;
    ByteArrayOutputStream output;

    public WrapperWriter(ByteArrayOutputStream out, HttpServletResponse response) {
        super(out);
        this.response = response;
        this.output = out;
    }

    @Override
    public void write(int c) {
        super.write(c);
        try {
            response.getWriter().write(c);
        } catch (IOException e) {
            e.printStackTrace();
            this.setError();
        }
    }

    @Override
    public void write(String s, int off, int len) {
        super.write(s, off, len);
        try {
            response.getWriter().write(s, off, len);
        } catch (IOException e) {
            e.printStackTrace();
            this.setError();
        }
    }
}
