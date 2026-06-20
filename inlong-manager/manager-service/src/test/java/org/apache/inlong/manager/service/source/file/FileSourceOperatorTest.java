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

package org.apache.inlong.manager.service.source.file;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.source.file.FileSourceRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.source.StreamSourceService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Unit test for FileSourceOperator path traversal validation.
 */
public class FileSourceOperatorTest extends ServiceBaseTest {

    private static final String TEST_GROUP = "file_source_test_group";
    private static final String TEST_STREAM = "file_source_test_stream";

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    @BeforeEach
    void setUp() {
        streamServiceTest.saveInlongStream(TEST_GROUP, TEST_STREAM, GLOBAL_OPERATOR);
    }

    @Test
    void testNormalPatternShouldPass() {
        FileSourceRequest request = new FileSourceRequest();
        request.setInlongGroupId(TEST_GROUP);
        request.setInlongStreamId(TEST_STREAM);
        request.setSourceName("normal-file-source");
        request.setSourceType(SourceType.FILE);
        request.setPattern("/data/logs/app.*.log");

        Assertions.assertDoesNotThrow(() -> sourceService.save(request, GLOBAL_OPERATOR));
    }

    @Test
    void testSimpleTraversalShouldReject() {
        FileSourceRequest request = new FileSourceRequest();
        request.setInlongGroupId(TEST_GROUP);
        request.setInlongStreamId(TEST_STREAM);
        request.setSourceName("traversal-source-1");
        request.setSourceType(SourceType.FILE);
        request.setPattern("../../../../etc/passwd");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> sourceService.save(request, GLOBAL_OPERATOR));
        Assertions.assertTrue(ex.getMessage().contains("Path traversal detected"));
    }

    @Test
    void testMixedPathWithTraversalShouldReject() {
        FileSourceRequest request = new FileSourceRequest();
        request.setInlongGroupId(TEST_GROUP);
        request.setInlongStreamId(TEST_STREAM);
        request.setSourceName("traversal-source-2");
        request.setSourceType(SourceType.FILE);
        request.setPattern("/data/logs/../../etc/passwd");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> sourceService.save(request, GLOBAL_OPERATOR));
        Assertions.assertTrue(ex.getMessage().contains("Path traversal detected"));
    }

    @Test
    void testBackslashTraversalShouldReject() {
        FileSourceRequest request = new FileSourceRequest();
        request.setInlongGroupId(TEST_GROUP);
        request.setInlongStreamId(TEST_STREAM);
        request.setSourceName("traversal-source-3");
        request.setSourceType(SourceType.FILE);
        request.setPattern("..\\..\\etc\\passwd");

        BusinessException ex = Assertions.assertThrows(BusinessException.class,
                () -> sourceService.save(request, GLOBAL_OPERATOR));
        Assertions.assertTrue(ex.getMessage().contains("Path traversal detected"));
    }

    @Test
    void testDotInFilenameShouldPass() {
        FileSourceRequest request = new FileSourceRequest();
        request.setInlongGroupId(TEST_GROUP);
        request.setInlongStreamId(TEST_STREAM);
        request.setSourceName("dotfile-source");
        request.setSourceType(SourceType.FILE);
        request.setPattern("/data/logs/app.log");

        Assertions.assertDoesNotThrow(() -> sourceService.save(request, GLOBAL_OPERATOR));
    }

    @Test
    void testDatePatternShouldPass() {
        FileSourceRequest request = new FileSourceRequest();
        request.setInlongGroupId(TEST_GROUP);
        request.setInlongStreamId(TEST_STREAM);
        request.setSourceName("date-pattern-source");
        request.setSourceType(SourceType.FILE);
        request.setPattern("/data/YYYYMMDD/app.*.log");

        Assertions.assertDoesNotThrow(() -> sourceService.save(request, GLOBAL_OPERATOR));
    }
}