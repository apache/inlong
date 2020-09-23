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

import java.util.List;
import java.util.Optional;
import org.apache.tubemq.manager.entry.BusinessEntry;
import org.apache.tubemq.manager.repository.BusinessRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/business")
public class BusinessController {

    @Autowired
    private BusinessRepository businessRepository;

    /**
     * add new business.
     *
     * @return - businessResult
     * @throws Exception - exception
     */
    @PostMapping("/add")
    public ResponseEntity<?> addBusiness(@RequestBody BusinessEntry entry) throws Exception {
        // businessRepository.saveAndFlush(entry);
        return ResponseEntity.ok().build();
    }

    /**
     * update business
     *
     * @return
     * @throws Exception
     */
    @PostMapping("/update")
    public ResponseEntity<?> updateBusiness(@RequestBody BusinessEntry entry) throws Exception {
        return ResponseEntity.ok().build();
    }

    /**
     * Check business status by business name.
     *
     * @return
     * @throws Exception
     */
    @GetMapping("/check")
    public ResponseEntity<?> checkBusinessByName(
            @RequestParam String businessName) throws Exception {
        List<BusinessEntry> result = businessRepository.findAllByBusinessName(businessName);
        return ResponseEntity.ok().build();
    }

    /**
     * get business by id.
     *
     * @param id business id
     * @return BusinessResult
     * @throws Exception
     */
    @GetMapping("/get/{id}")
    public ResponseEntity<BusinessEntry> getBusinessByID(
            @PathVariable Long id) throws Exception {
        Optional<BusinessEntry> businessEntry = businessRepository.findById(id);
        if (businessEntry.isPresent()) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}
