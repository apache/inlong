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

package org.apache.inlong.tool.task;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.evaluator.AlertEvaluator;
import org.apache.inlong.audit.tool.manager.AuditAlertRuleManager;
import org.apache.inlong.audit.tool.task.AuditCheckTask;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class AuditCheckTaskTest {

    @Mock
    private AuditAlertRuleManager auditAlertRuleManager = AuditAlertRuleManager.getInstance();

    @Mock
    private AlertEvaluator alertEvaluator;

    @Mock
    private AppConfig appConfig;

    @Mock
    private Properties properties;

    private AuditCheckTask auditCheckTask;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Mock AppConfig behavior
        when(appConfig.getProperties()).thenReturn(properties);
        when(properties.getProperty("audit.data.time.delay.minute", "1")).thenReturn("1");
        when(properties.getProperty("audit.data.time.interval.minute", "1")).thenReturn("5");
        when(properties.getProperty("audit.id.source", "5")).thenReturn("5");

        // Create AuditCheckTask instance
        auditCheckTask = new AuditCheckTask(auditAlertRuleManager, alertEvaluator, appConfig);
    }

    @Test
    public void testConstructor() {
        // Verify that the object is created successfully
        assertNotNull(auditCheckTask);

        // Verify that the properties were read correctly
        verify(appConfig, times(3)).getProperties();
        verify(properties, times(1)).getProperty("audit.data.time.delay.minute", "1");
        verify(properties, times(1)).getProperty("audit.data.time.interval.minute", "1");
        verify(properties, times(1)).getProperty("audit.id.source", "5");
    }

    @Test
    public void testStartMethod() throws NoSuchFieldException, IllegalAccessException {
        // Start the task
        auditCheckTask.start();

        // Access the scheduler field to verify it's not null
        Field schedulerField = AuditCheckTask.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        ScheduledExecutorService scheduler = (ScheduledExecutorService) schedulerField.get(auditCheckTask);

        // Verify that the scheduler is not null
        assertNotNull(scheduler);

        // Stop the scheduler to clean up
        auditCheckTask.stop();
    }

    @Test
    public void testStopMethod() {
        // Start the task first
        auditCheckTask.start();

        // Stop the task
        auditCheckTask.stop();

        // We can't easily verify the internal state without exposing it,
        // but we can ensure the method runs without exception
        // In a real test, we might use a more sophisticated approach to verify shutdown
    }

    @Test
    public void testConstructorWithNullAppConfig() {
        // Test constructor with null AppConfig
        AuditCheckTask task = new AuditCheckTask(auditAlertRuleManager, alertEvaluator, null);
        assertNotNull(task);
    }

    @Test
    public void testConstructorWithNullProperties() {
        // Test constructor with null properties
        when(appConfig.getProperties()).thenReturn(null);
        AuditCheckTask task = new AuditCheckTask(auditAlertRuleManager, alertEvaluator, appConfig);
        assertNotNull(task);
    }

    @Test
    public void testConstructorWithInvalidInterval() {
        // Test constructor with invalid interval value
        when(properties.getProperty("audit.data.time.interval.minute")).thenReturn("invalid");
        AuditCheckTask task = new AuditCheckTask(auditAlertRuleManager, alertEvaluator, appConfig);
        assertNotNull(task);
    }

    @Test
    public void testConstructorWithEmptyInterval() {
        // Test constructor with empty interval value
        when(properties.getProperty("audit.data.time.interval.minute")).thenReturn("");
        AuditCheckTask task = new AuditCheckTask(auditAlertRuleManager, alertEvaluator, appConfig);
        assertNotNull(task);
    }
}
