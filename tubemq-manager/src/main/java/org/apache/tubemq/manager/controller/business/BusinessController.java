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
package org.apache.tubemq.manager.controller.business;

import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.entry.BusinessEntry;
import org.apache.tubemq.manager.exceptions.TubeMQManagerException;
import org.apache.tubemq.manager.repository.BusinessRepository;
import org.apache.tubemq.manager.service.AsyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/business")
@Slf4j
public class BusinessController {

    @Autowired
    private BusinessRepository businessRepository;

    @Autowired
    private AsyncService asyncService;

    /**
     * add new business.
     *
     * @return - businessResult
     * @throws Exception - exception
     */
    @PostMapping("/add")
    public BusinessResult addBusiness(@RequestBody BusinessEntry entry) {
        businessRepository.saveAndFlush(entry);
        return new BusinessResult();
    }

    /**
     * update business
     *
     * @return
     * @throws Exception
     */
    @PostMapping("/update")
    public BusinessResult updateBusiness(@RequestBody BusinessEntry entry) {
        return new BusinessResult();
    }

    /**
     * Check business status by business name.
     *
     * @return
     * @throws Exception
     */
    @GetMapping("/check")
    public BusinessResult checkBusinessByName(
            @RequestParam String businessName) {
        List<BusinessEntry> result = businessRepository.findAllByBusinessName(businessName);
        return new BusinessResult();
    }

    /**
     * get business by id.
     *
     * @param id business id
     * @return BusinessResult
     * @throws Exception
     */
    @GetMapping("/get/{id}")
    public BusinessResult getBusinessByID(
            @PathVariable Long id) {
        Optional<BusinessEntry> businessEntry = businessRepository.findById(id);
        BusinessResult result = new BusinessResult();
        if (!businessEntry.isPresent()) {
            result.setCode(-1);
            result.setMessage("business not found");
        }
        return result;
    }


    @GetMapping("/throwException")
    public BusinessResult throwException() {
        throw new TubeMQManagerException("exception for test");
    }
}
