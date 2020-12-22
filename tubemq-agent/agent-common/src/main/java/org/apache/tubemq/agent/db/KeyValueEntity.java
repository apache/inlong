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
package org.apache.tubemq.agent.db;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * key value entity. key is string and value is a json
 */
@Entity(version = 1)
public class KeyValueEntity {

    @PrimaryKey
    private String key;


    @SecondaryKey(relate = Relationship.MANY_TO_ONE)
    private StateSearchKey stateSearchKey;

    // string json
    private String jsonValue;

    private KeyValueEntity() {

    }

    public KeyValueEntity(String key, String jsonValue) {
        this.key = key;
        this.jsonValue = jsonValue;
        this.stateSearchKey = StateSearchKey.ACCEPTED;
    }

    public String getKey() {
        return key;
    }

    public StateSearchKey getStateSearchKey() {
        return stateSearchKey;
    }

    public KeyValueEntity setStateSearchKey(StateSearchKey stateSearchKey) {
        this.stateSearchKey = stateSearchKey;
        return this;
    }

    public String getJsonValue() {
        return jsonValue;
    }

    public KeyValueEntity setJsonValue(String jsonValue) {
        this.jsonValue = jsonValue;
        return this;
    }
}
