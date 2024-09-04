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

package org.apache.inlong.sdk.transform.process.function.string;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
/**
 * TestArithmeticFunctionProcessor
 * description: test all the string functions in transform processor
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestInsertFunction.class, TestLeftFunction.class, TestLengthFunction.class,
        TestLocateFunction.class, TestLowerFunction.class, TestLpadFunction.class, TestReplaceFunction.class,
        TestReplicateFunction.class, TestReverseFunction.class, TestRightFunction.class, TestRpadFunction.class,
        TestSpaceFunction.class, TestStrcmpFunction.class, TestSubstringFunction.class, TestToBase64Function.class,
        TestTranslateFunction.class, TestTrimFunction.class, TestUpperFunction.class})
public class TestStringFunctionsProcessor {
}
