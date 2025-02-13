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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.network.PkgCacheQuota;
import org.apache.inlong.sdk.dataproxy.utils.Tuple2;

import org.junit.Assert;
import org.junit.Test;

public class PkgCacheQuotaTest {

    @Test
    public void testPkgCacheQuota() throws Exception {
        ProcessResult procResult = new ProcessResult();
        PkgCacheQuota quota1 =
                new PkgCacheQuota(false, "q1", -1, -1, 20);
        Assert.assertTrue(quota1.tryAcquire(30, procResult));
        Assert.assertTrue(quota1.tryAcquire(1000000, procResult));
        quota1.release(30);
        Tuple2<Integer, Integer> result = quota1.getPkgCacheAvailQuota();
        Assert.assertEquals(result.getF0().intValue(), SdkConsts.UNDEFINED_VALUE);
        Assert.assertEquals(result.getF1().intValue(), SdkConsts.UNDEFINED_VALUE);

        PkgCacheQuota quota2 = new PkgCacheQuota(true, "q2", 3, -1, 20);
        Assert.assertTrue(quota2.tryAcquire(30, procResult));
        Assert.assertTrue(quota2.tryAcquire(50, procResult));
        Assert.assertTrue(quota2.tryAcquire(40, procResult));
        Assert.assertFalse(quota2.tryAcquire(50, procResult));
        result = quota2.getPkgCacheAvailQuota();
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getF0());
        Assert.assertEquals(result.getF1().intValue(), SdkConsts.UNDEFINED_VALUE);
        Assert.assertEquals(result.getF0().intValue(), 0);
        quota2.release(50);
        result = quota2.getPkgCacheAvailQuota();
        Assert.assertEquals(result.getF0().intValue(), 1);

        PkgCacheQuota quota3 = new PkgCacheQuota(false, "q3", -1, 5, 1014);
        Assert.assertTrue(quota3.tryAcquire(10, procResult));
        Assert.assertTrue(quota3.tryAcquire(20, procResult));
        Assert.assertTrue(quota3.tryAcquire(30, procResult));
        Assert.assertFalse(quota3.tryAcquire(40000, procResult));
        Assert.assertFalse(quota3.tryAcquire(50, procResult));
        result = quota3.getPkgCacheAvailQuota();
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getF0().intValue(), SdkConsts.UNDEFINED_VALUE);
        Assert.assertEquals(result.getF1().intValue(), 0);
        quota3.release(50);
        result = quota3.getPkgCacheAvailQuota();
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getF0().intValue(), SdkConsts.UNDEFINED_VALUE);
        Assert.assertEquals(result.getF1().intValue(), 2);

        PkgCacheQuota quota4 = new PkgCacheQuota(true, "pa5", 5, 5, 1014);
        Assert.assertTrue(quota4.tryAcquire(10, procResult));
        Assert.assertTrue(quota4.tryAcquire(20, procResult));
        Assert.assertTrue(quota4.tryAcquire(30, procResult));
        Assert.assertFalse(quota4.tryAcquire(40000, procResult));
        Assert.assertFalse(quota4.tryAcquire(50, procResult));
        result = quota4.getPkgCacheAvailQuota();
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getF0().intValue(), 2);
        Assert.assertEquals(result.getF1().intValue(), 0);
        quota4.release(50);
        result = quota4.getPkgCacheAvailQuota();
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getF0().intValue(), 3);
        Assert.assertEquals(result.getF1().intValue(), 2);
    }
}
