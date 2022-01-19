package org.apache.inlong.manager.service.workflow.business;

import org.junit.runner.RunWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;


@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = "org.apache.inlong.manager")
@SpringBootTest
@RunWith(SpringRunner.class)
@ActiveProfiles(value = {"test"})
public class BaseTest {

    public static void main(String[] args){
        SpringApplication.run(BaseTest.class, args);
    }
}
