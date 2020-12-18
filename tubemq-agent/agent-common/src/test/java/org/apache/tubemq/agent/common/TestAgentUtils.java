/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.common;

import org.apache.tubemq.agent.utils.AgentDBUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestAgentUtils {

    @Test
    public void testReplaceDynamicSequence() throws Exception {
        String[] result = AgentDBUtils.replaceDynamicSequence("${1, 99}");
        assert result != null;
        String[] expectResult = new String[99];
        for (int index = 1; index < 100; index++) {
            expectResult[index - 1] = String.valueOf(index);
        }
        Assert.assertArrayEquals(expectResult, result);

        result = AgentDBUtils.replaceDynamicSequence("${0x0, 0xf}");
        expectResult = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a",
            "b", "c", "d", "e", "f"};
        Assert.assertArrayEquals(expectResult, result);

        result = AgentDBUtils.replaceDynamicSequence("${O01,O10}");
        expectResult = new String[]{"01", "02", "03", "04", "05",
            "06", "07", "10"};
        Assert.assertArrayEquals(expectResult, result);
    }
}
