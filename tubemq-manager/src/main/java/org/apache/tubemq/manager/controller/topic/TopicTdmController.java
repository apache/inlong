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
package org.apache.tubemq.manager.controller.topic;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.entry.TopicEntry;
import org.apache.tubemq.manager.entry.TopicStatus;
import org.apache.tubemq.manager.exceptions.TubeMQManagerException;
import org.apache.tubemq.manager.repository.TopicRepository;
import org.apache.tubemq.manager.service.TopicBackendWorker;
import org.apache.tubemq.manager.service.TopicFuture;
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
public class TopicTdmController {

    @Autowired
    private TopicRepository topicRepository;

    @Autowired
    private TopicBackendWorker topicBackendWorker;

    /**
     * add new topic.
     *
     * @return - businessResult
     * @throws Exception - exception
     */
    @PostMapping("/add")
    public TubeMQResult addTopic(@RequestBody TopicEntry entry) {
        // entry in adding status
        entry.setStatus(TopicStatus.ADDING.value());
        topicRepository.saveAndFlush(entry);
        CompletableFuture<TopicEntry> future = new CompletableFuture<>();
        topicBackendWorker.addTopicFuture(new TopicFuture(entry, future));
        future.whenComplete((entry1, throwable) -> {
            entry1.setStatus(TopicStatus.SUCCESS.value());
            if (throwable != null) {
                // if throwable is not success, mark it as failed.
                entry1.setStatus(TopicStatus.FAILED.value());
                log.error("exception caught", throwable);
            }
            topicRepository.saveAndFlush(entry1);
        });
        return new TubeMQResult();
    }

    /**
     * update topic
     *
     * @return
     * @throws Exception
     */
    @PostMapping("/update")
    public TubeMQResult updateTopic(@RequestBody TopicEntry entry) {
        return new TubeMQResult();
    }

    /**
     * Check topic status by business name.
     *
     * @return
     * @throws Exception
     */
    @GetMapping("/check")
    public TubeMQResult checkTopicByBusinessName(
            @RequestParam String businessName) {
        List<TopicEntry> result = topicRepository.findAllByBusinessName(businessName);
        return new TubeMQResult();
    }

    /**
     * get topic by id.
     *
     * @param id business id
     * @return BusinessResult
     * @throws Exception
     */
    @GetMapping("/get/{id}")
    public TubeMQResult getBusinessByID(
            @PathVariable Long id) {
        Optional<TopicEntry> businessEntry = topicRepository.findById(id);
        TubeMQResult result = new TubeMQResult();
        if (!businessEntry.isPresent()) {
            result.setErrCode(-1);
            result.setErrMsg("business not found");
        }
        return result;
    }

    /**
     * test for exception situation.
     * @return
     */
    @GetMapping("/throwException")
    public TubeMQResult throwException() {
        throw new TubeMQManagerException("exception for test");
    }
}
