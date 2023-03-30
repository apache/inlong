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

package org.apache.inlong.manager.pojo.stream;

import org.apache.inlong.manager.common.tool.excel.validator.ExcelCellValidator;
import org.apache.poi.hssf.usermodel.DVConstraint;

import static org.apache.inlong.manager.common.consts.InlongConstants.STREAM_FIELD_TYPES;

/**
 * This class is used to validate the stream field type in the business product.
 */
public class StreamFieldTypeExcelValidator implements ExcelCellValidator<String> {

    public StreamFieldTypeExcelValidator() {
    }

    /**
     * Returns the constraint for the validator
     * @return DVConstraint
     */
    @Override
    public org.apache.poi.hssf.usermodel.DVConstraint constraint() {
        return DVConstraint.createExplicitListConstraint(STREAM_FIELD_TYPES.toArray(new String[0]));
    }

    /**
     * Validates the given object
     * @param o The object to validate
     * @return boolean
     */
    @Override
    public boolean validate(String o) {
        if (o == null) {
            return false;
        }
        return STREAM_FIELD_TYPES.contains(o);
    }

    /**
     * Returns the invalid tip for the validator
     * @return String
     */
    @Override
    public String getInvalidTip() {
        return "Incorrect field type: must be one of " + STREAM_FIELD_TYPES;
    }
}
