package org.apache.tubemq.manager;

import org.apache.tubemq.manager.controller.TopicController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class TestingWebApplicationTests {
    @Autowired
    private TopicController topicController;

    @Test
    public void contextLoads() throws Exception {
        assertThat(topicController).isNotNull();
    }
}
