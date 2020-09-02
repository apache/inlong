package org.apache.tubemq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories(basePackages = {"org.apache.tubemq.manager"})
public class TubeMQManager {
    public static void main(String[] args) {
        SpringApplication.run(TubeMQManager.class, args);
    }
}