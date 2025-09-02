package org.apache.inlong.audit.tool.manager;

import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;
import org.apache.inlong.audit.tool.metric.AuditData;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.audit.tool.response.Response;

import java.util.List;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerClient {

    private final AppConfig appConfig;
    private final PrometheusReporter prometheusReporter;
    private final OpenTelemetryReporter openTelemetryReporter;

    public ManagerClient(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.prometheusReporter = new PrometheusReporter(appConfig.getPrometheusConfig());
        this.openTelemetryReporter = new OpenTelemetryReporter(appConfig.getOpenTelemetryConfig());
    }

    public List<AlertPolicy> fetchAlertPolicies() {
        String managerUrl = appConfig.getManagerUrl(); // "http://localhost:8080"
        String path = "/audit/alert/rule/list";
        // 确保只出现一个斜杠
        String fullUrl = (managerUrl.endsWith("/") ? managerUrl.substring(0, managerUrl.length() - 1) : managerUrl) + path;
        //发送http请求manger API获取AuditAlertRule告警策略
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/json")
                .GET()
                .build();

        Response<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            return mapper.readValue(response.getData(), new TypeReference<List<AlertPolicy>>() {});
        } else {
            LOGGER.error("fetchAlertPolicies fail "  + ": " + response.getData());
            return null;
        }
    }


    public <List<AuditData>> fetchAuditData() {
        List<AlertPolicy> auditAlertRules = fetchAlertPolicies();
        List<AuditData> auditDataList = new ArrayList<>();
        for(AuditAlertRule auditAlertRule :  auditAlertRules){
            ObjectMapper mapper = new ObjectMapper();
            AuditRequest auditRequest = new AuditRequest();
            auditRequest.setInlongGroupId(auditAlertRules.getInlongGroupId());
            auditRequest.setInlongStreamId(auditAlertRules.getInlongStreamId());
            auditRequest.setAuditId(auditAlertRules.getAuditId());

            String managerUrl = appConfig.getManagerUrl(); // "http://localhost:8080"
            String path = "/audit/listAll";
            // 确保只出现一个斜杠
            String fullUrl = (managerUrl.endsWith("/") ? managerUrl.substring(0, managerUrl.length() - 1) : managerUrl) + path;

            String jsonBody = mapper.writeValueAsString(auditRequest);

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            Response<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                auditDataList.add(mapper.readValue(response.getData(), new TypeReference<List<AuditData>>() {}));
            } else {
                LOGGER.error("fetchAlertPolicies fail " + ": " + response.getData());
            }
        }
        return auditDataList;
    }
}