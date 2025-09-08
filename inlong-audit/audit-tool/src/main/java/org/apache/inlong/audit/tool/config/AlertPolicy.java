package org.apache.inlong.audit.tool.config;

import java.util.List;


public class AlertPolicy {
    private String name;
    private String description;
    private double threshold;
    private String comparisonOperator;
    private String alertType;

    public AlertPolicy(String name, String description, double threshold, String comparisonOperator, String alertType) {
        this.name = name;
        this.description = description;
        this.threshold = threshold;
        this.comparisonOperator = comparisonOperator;
        this.alertType = alertType;
    }

    public double getThreshold() {
        return threshold;
    }

    public String getComparisonOperator() {
        return comparisonOperator;
    }

    public String getAlertType() {
        return alertType;
    }

    @Override
    public String toString() {
        return "AlertPolicy{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", threshold=" + threshold +
                ", comparisonOperator='" + comparisonOperator + '\'' +
                ", alertType='" + alertType + '\'' +
                '}';
    }

    public List<String> getTargets() {
        return null;
    }
}