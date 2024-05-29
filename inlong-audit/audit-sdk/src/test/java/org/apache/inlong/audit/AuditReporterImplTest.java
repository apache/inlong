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

package org.apache.inlong.audit;

import org.junit.Test;

import static org.apache.inlong.audit.AuditIdEnum.AGENT_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SORT_HIVE_INPUT;
import static org.junit.Assert.assertEquals;

public class AuditReporterImplTest {

    @Test
    public void TestBuildAuditId() {
        int auditId = AuditOperator.getInstance().buildSuccessfulAuditId(AGENT_INPUT);
        assertEquals(3, auditId);
        auditId = AuditOperator.getInstance().buildFailedAuditId(AGENT_INPUT);
        assertEquals(524291, auditId);
        auditId = AuditOperator.getInstance().buildRetryAuditId(AGENT_INPUT);
        assertEquals(65539, auditId);
        auditId = AuditOperator.getInstance().buildDiscardAuditId(AGENT_INPUT);
        assertEquals(131075, auditId);

        auditId = AuditOperator.getInstance().buildSuccessfulAuditId(SORT_HIVE_INPUT, false);
        assertEquals(262151, auditId);
        auditId = AuditOperator.getInstance().buildFailedAuditId(SORT_HIVE_INPUT, false);
        assertEquals(786439, auditId);
        auditId = AuditOperator.getInstance().buildDiscardAuditId(SORT_HIVE_INPUT, false);
        assertEquals(393223, auditId);
        auditId = AuditOperator.getInstance().buildRetryAuditId(SORT_HIVE_INPUT, false);
        assertEquals(327687, auditId);
    }
}
