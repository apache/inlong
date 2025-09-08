package org.apache.inlong.audit.tool.config;

import org.apache.inlong.audit.tool.manager.ManagerClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.inlong.audit.tool.config.ConfigConstants.DEFAULT_PROMETHEUS_PORT;
import static org.apache.inlong.audit.tool.config.ConfigConstants.KEY_PROMETHEUS_PORT;

public class AppConfig {
    private Properties properties;
    private ManagerClient managerClient;

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

    public Map<String, Object> getPrometheusConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("prometheus.enabled", Boolean.parseBoolean(properties.getProperty("prometheus.enabled", "false")));
        config.put("prometheus.endpoint", properties.getProperty("prometheus.endpoint", "http://localhost:9090/api/v1/write"));
        Integer defaultPrometheusPort = DEFAULT_PROMETHEUS_PORT;
        config.put(KEY_PROMETHEUS_PORT, Integer.parseInt(properties.getProperty(KEY_PROMETHEUS_PORT, defaultPrometheusPort.toString())));
        System.out.println("Prometheus port: " + config.get(KEY_PROMETHEUS_PORT));
        return config;
    }
    public Properties getProperties(){
        return properties;
    }
}