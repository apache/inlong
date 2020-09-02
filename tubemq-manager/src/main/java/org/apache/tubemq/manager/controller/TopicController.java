package org.apache.tubemq.manager.controller;

import org.apache.tubemq.manager.model.Topic;
import org.apache.tubemq.manager.repository.TopicRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Interface for topic manager
 */
@RestController
@RequestMapping("/topic")
public class TopicController {

    @Autowired
    TopicRepository topicRepository;

    @GetMapping("/list")
    public List<Topic> getAllTopics() {
        return topicRepository.findAll();
    }

    @GetMapping("/{id}")
    public Topic getTopicById(@PathVariable(value = "id") Long topicId) {
        return topicRepository.findById(topicId).orElse(null);
    }
}
