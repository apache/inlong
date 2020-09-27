/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.controller;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.manager.controller.business.BusinessResult;
import org.apache.tubemq.manager.exceptions.TubeMQManagerException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Controller advice for handling exceptions
 */
@RestControllerAdvice
public class ManagerControllerAdvice {

    /**
     * handling business TubeMQManagerException, and return json format string.
     *
     * @param request - http request
     * @param ex - exception
     * @return entity
     */
    @ExceptionHandler(TubeMQManagerException.class)
    public BusinessResult handlingBusinessException(HttpServletRequest request,
            TubeMQManagerException ex) {
        BusinessResult result = new BusinessResult();
        result.setMessage(ex.getMessage());
        result.setCode(-1);
        return result;
    }
}
