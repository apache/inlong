package org.apache.inlong.audit.tool.config;

import org.apache.inlong.audit.tool.manager.ManagerClient;

import java.util.Properties;

public class AppConfig {
    private Properties properties;


    public AppConfig() {
        properties = new Properties();
        loadProperties();

    }

    private void loadProperties() {
        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application properties", e);
        }
    }

    public String getManagerUrl() {
        return properties.getProperty("manager.url");
    }

    public String getAlertPolicyConfig() {
        return properties.getProperty("alert.policy.config");
    }


}