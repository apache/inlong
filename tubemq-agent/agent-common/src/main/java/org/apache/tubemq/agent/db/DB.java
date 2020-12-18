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

import java.util.List;
import javax.management.openmbean.KeyAlreadyExistsException;

/**
 * local storage for key/value.
 */
public interface DB {

    /**
     * get keyValue by key
     *
     * @param key - key
     * @return key/value
     * @throws NullPointerException key should not be null.
     */
    KeyValueEntity get(String key);

    /**
     * store keyValue, if key has exists, throw exception.
     *
     * @param entity - key/value
     * @throws NullPointerException key should not be null
     * @throws KeyAlreadyExistsException key already exists
     */
    void set(KeyValueEntity entity);

    /**
     * store keyValue, if key has exists, overwrite it.
     *
     * @param entity - key/value
     * @return null or old value which is overwritten.
     * @throws NullPointerException key should not be null.
     */
    KeyValueEntity put(KeyValueEntity entity);

    /**
     * remove keyValue by key.
     *
     * @param key - key
     * @return key/value
     * @throws NullPointerException key should not be null.
     */
    KeyValueEntity remove(String key);

    /**
     * search keyValue list by search key.
     *
     * @param searchKey - search keys.
     * @return key/value list
     * @throws NullPointerException search key should not be null.
     */
    List<KeyValueEntity> search(StateSearchKey searchKey);

    /**
     * search one keyValue by search key
     * @param searchKey - search key
     * @return null or keyValue
     */
    KeyValueEntity searchOne(StateSearchKey searchKey);

    /**
     * find all by prefix key.
     * @param prefix - prefix string
     * @return list of k/v
     */
    List<KeyValueEntity> findAll(String prefix);
}
