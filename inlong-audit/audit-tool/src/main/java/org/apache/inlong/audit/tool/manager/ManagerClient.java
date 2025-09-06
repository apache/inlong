package org.apache.inlong.audit.tool.manager;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.audit.tool.config.AppConfig;
import org.apache.inlong.audit.tool.config.AlertPolicy;
import org.apache.inlong.audit.tool.reporter.PrometheusReporter;
import org.apache.inlong.audit.tool.reporter.OpenTelemetryReporter;
import org.apache.inlong.audit.tool.DTO.AuditData;
import org.apache.inlong.audit.tool.util.CommonBeanUtils;
import org.apache.inlong.audit.tool.util.HttpUtils;
//import org.apache.inlong.manager.pojo.audit.*;
import org.apache.inlong.audit.tool.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
//import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
//import org.springframework.web.client.RestTemplate;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.stream.Collectors;

@Service
public class ManagerClient {

//    private final AppConfig appConfig;
//    private final PrometheusReporter prometheusReporter;
//    private final OpenTelemetryReporter openTelemetryReporter;
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerClient.class);
//    @Autowired
//    private RestTemplate restTemplate;

//    public ManagerClient(AppConfig appConfig) {
//        this.appConfig = appConfig;
//        this.prometheusReporter = new PrometheusReporter(appConfig.getPrometheusConfig());
//        this.openTelemetryReporter = new OpenTelemetryReporter(appConfig.getOpenTelemetryConfig());
//    }
//
//    public List<AlertPolicy> fetchAlertPolicies() throws Exception {
//        List<AuditAlertRule> auditAlertRules = fetchAlertRules();
//        return CommonBeanUtils.copyListProperties(auditAlertRules, AlertPolicy::new);
//    }
//
//    public List<AuditAlertRule> fetchAlertRules(){
//        String managerUrl = appConfig.getManagerUrl();
//        String path = "/audit/alert/rule/list";
//        // 确保只出现一个斜杠
//        String url = (managerUrl.endsWith("/") ? managerUrl.substring(0, managerUrl.length() - 1) : managerUrl) + path;
//        //发送http请求manger API获取AuditAlertRule告警策略
//        Response<List<AuditAlertRule>> result = HttpUtils.request(restTemplate,
//                url,
//                HttpMethod.GET, null,
//                null,
//                new ParameterizedTypeReference<>() {});
//        LOGGER.info("success to query audit info for url ={}", url);
//
//        if (result.isSuccess()) {
//            return CommonBeanUtils.copyListProperties(result.getData(), AuditAlertRule::new);
//        } else {
//            LOGGER.error("fetchAlertPolicies fail "  + ": " + result.getData());
//            return null;
//        }
//    }
//
//
//    public List<AuditData> fetchAuditData() throws Exception {
//        List<AuditAlertRule> auditAlertRules = fetchAlertRules();
//        List<AuditData> auditDataList = new ArrayList<>();
//        for(AuditAlertRule auditAlertRule :  auditAlertRules){
//            ObjectMapper mapper = new ObjectMapper();
//            AuditRequest auditRequest = new AuditRequest();
//            auditRequest.setInlongGroupId(auditAlertRule.getInlongGroupId());
//            auditRequest.setInlongStreamId(auditAlertRule.getInlongStreamId());
//            auditRequest.setAuditIds(Arrays.stream(auditAlertRule.getAuditId().split(","))
//                    .map(String::trim) //
//                    .collect(Collectors.toList()));
//
//            String managerUrl = appConfig.getManagerUrl(); // "http://localhost:8080"
//            String path = "/audit/listAll";
//            // 确保只出现一个斜杠
//            String url = (managerUrl.endsWith("/") ? managerUrl.substring(0, managerUrl.length() - 1) : managerUrl) + path;
//
//            String jsonBody = mapper.writeValueAsString(auditRequest);
//
//            Response<List<AuditVO>> result = HttpUtils.request(restTemplate,
//                    url,
//                    HttpMethod.POST, jsonBody,
//                    null,
//                    new ParameterizedTypeReference<>(){});
//            LOGGER.info("success to query audit info for url ={}", url);
//            if (result.isSuccess()) {
//                return CommonBeanUtils.copyListProperties(result.getData(), AuditData::new);
//            } else {
//                LOGGER.error("fetchAuditData fail " + ": " + result.getData());
//            }
//        }
//        return auditDataList;
//    }
}