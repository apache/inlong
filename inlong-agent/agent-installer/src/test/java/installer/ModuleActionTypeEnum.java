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

package installer;

/**
 * Enum of module action.
 */
public enum ModuleActionTypeEnum {

    DOWNLOAD(0),
    INSTALL(1),
    UNINSTALL(2),
    START(3),
    STOP(4);

    private final int type;

    ModuleActionTypeEnum(int state) {
        this.type = state;
    }

    public static ModuleActionTypeEnum getTaskState(int state) {
        switch (state) {
            case 0:
                return DOWNLOAD;
            case 1:
                return INSTALL;
            case 2:
                return UNINSTALL;
            case 3:
                return START;
            case 4:
                return STOP;
            default:
                throw new RuntimeException("Unsupported module action " + state);
        }
    }

    public int getType() {
        return type;
    }
}
