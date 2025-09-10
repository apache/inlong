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

package org.apache.inlong.audit.tool.DTO;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class AlertPolicy {

    private String name;
    private String description;
    private double threshold;
    private String comparisonOperator;
    private String alertType;

    public AlertPolicy(String name, String description, double threshold, String comparisonOperator, String alertType) {
        this.name = name;
        this.description = description;
        this.threshold = threshold;
        this.comparisonOperator = comparisonOperator;
        this.alertType = alertType;
    }

    public AlertPolicy() {

    }

    @Override
    public String toString() {
        return "AlertPolicy{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", threshold=" + threshold +
                ", comparisonOperator='" + comparisonOperator + '\'' +
                ", alertType='" + alertType + '\'' +
                '}';
    }

    public List<String> getTargets() {
        return null;
    }

}