/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.server.common.utils.WebParameterUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WebParameterTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void validParameterTest() throws Exception {

        DateFormat sdf = new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
        Date date = WebParameterUtils.validDateParameter("test",
                "20190101012259", 20, true, new Date());
        Assert.assertEquals(date, sdf.parse("20190101012259"));

        String ip = WebParameterUtils.validAddressParameter("testIp",
                "10.1.1.1", 20, true, "10.0.0.0");
        Assert.assertEquals("10.1.1.1", ip);

        ip = WebParameterUtils.validAddressParameter("testIp",
                "", 20, false, "10.0.0.0");
        Assert.assertEquals("10.0.0.0", ip);
    }
}
