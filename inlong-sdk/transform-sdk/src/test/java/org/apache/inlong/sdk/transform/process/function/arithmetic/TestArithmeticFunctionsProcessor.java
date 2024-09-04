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

package org.apache.inlong.sdk.transform.process.function.arithmetic;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
/**
 * TestArithmeticFunctionProcessor
 * description: test all the arithmetic functions in transform processor
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestAbsFunction.class, TestAcosFunction.class, TestBinFunction.class, TestBinFunction.class,
        TestCeilFunction.class, TestCosFunction.class, TestExpFunction.class, TestFloorFunction.class,
        TestHexFunction.class, TestIfNullFunction.class, TestLnFunction.class, TestLog2Function.class,
        TestLogFunction.class, TestLog10Function.class, TestMd5Function.class, TestModuloFunction.class,
        TestPiFunction.class, TestPowerFunction.class, TestRadiansFunction.class, TestRandFunction.class,
        TestRoundFunction.class, TestSha2Function.class, TestShaFunction.class, TestSignFunction.class,
        TestSinFunction.class, TestSinhFunction.class, TestSqrtFunction.class, TestTanFunction.class
})
public class TestArithmeticFunctionsProcessor {
}
