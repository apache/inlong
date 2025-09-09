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

package org.apache.inlong.audit.tool.basemetric;

import io.prometheus.client.CollectorRegistry;
import org.apache.ibatis.session.SqlSession;
import org.apache.inlong.audit.tool.basemetric.manager.InlongAgentMetricsManager;
import org.apache.inlong.audit.tool.basemetric.manager.InlongApiMetricsManager;
import org.apache.inlong.audit.tool.basemetric.manager.InlongAuditMetricsManager;
import org.apache.inlong.audit.tool.basemetric.manager.InlongDataproxyMetricsManager;
import org.apache.inlong.audit.tool.basemetric.mapper.AuditMapper;
import org.apache.inlong.audit.tool.basemetric.util.AuditSQLUtil;
import org.apache.inlong.audit.tool.basemetric.vo.AuditDataVo;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class BaseMetricReporter {
    private Integer reportIntervalSeconds;
    private InlongAuditMetricsManager inlongAuditMetricsManager;
    private InlongApiMetricsManager inlongApiMetricsManager;
    private InlongAgentMetricsManager inlongAgentMetricsManager;
    private InlongDataproxyMetricsManager inlongDataproxyMetricsManager;

    public BaseMetricReporter(Properties properties, CollectorRegistry registry) {
        this.reportIntervalSeconds=Integer.parseInt(properties.getProperty("alert.policy.check.interval", "30"));
        this.inlongAuditMetricsManager = new InlongAuditMetricsManager(registry);
        this.inlongApiMetricsManager=new InlongApiMetricsManager(registry,reportIntervalSeconds);
        this.inlongAgentMetricsManager=new InlongAgentMetricsManager(registry,reportIntervalSeconds);
        this.inlongDataproxyMetricsManager=new InlongDataproxyMetricsManager(registry,reportIntervalSeconds);
    }

    public void reportBaseMetric(Boolean useFakeData) {
        List<AuditDataVo> auditDataVos=null;
        // Whether to use simulated fake data
        if(!useFakeData){
            //Do not use simulated data, search the database for the most recent audit data for indicator analysis
            SqlSession sqlSession = null;
            try {
                sqlSession = AuditSQLUtil.getSqlSession();
                AuditMapper auditMapper = sqlSession.getMapper(AuditMapper.class);
                auditDataVos = auditMapper.queryAuditDataBySeconds(reportIntervalSeconds);
            }catch (Exception e){
                e.printStackTrace();
            } finally {
                sqlSession.close();
            }
        }else{
            // Using simulated data
            auditDataVos = generateTestAuditDataVo(10000,0.1,0.1,
                    0.05,0.1,0.05);
        }

        List<AuditDataVo> apiAuditDataVos=new ArrayList<>();
        List<AuditDataVo> agentAuditDataVos=new ArrayList<>();
        List<AuditDataVo> dataproxyAuditDataVos=new ArrayList<>();
        for (AuditDataVo auditDataVo : auditDataVos) {
            String auditId = auditDataVo.getAuditId();
            if("1".equals(auditId)||"2".equals(auditId)){
                apiAuditDataVos.add(auditDataVo);
            }else if("3".equals(auditId)||"4".equals(auditId)){
                agentAuditDataVos.add(auditDataVo);
            }else if("5".equals(auditId)||"6".equals(auditId)){
                dataproxyAuditDataVos.add(auditDataVo);
            }
        }

        reportAuditMetric(auditDataVos);
        reportAuditMetric(auditDataVos);
        reportApiMetric(apiAuditDataVos);
        reportAgentMetric(agentAuditDataVos);
        reportDataproxyMetric(dataproxyAuditDataVos);
    }

    private void reportAuditMetric(List<AuditDataVo> auditDataVos){
        Long messageNum = 0L;
        Long messageSize = 0L;
        Long messageTotalDelay = 0L;
        Long messageVoNum=0L;
        for (AuditDataVo auditDataVo : auditDataVos){
            messageNum += auditDataVo.getCount();
            messageSize += auditDataVo.getSize();
            messageTotalDelay += auditDataVo.getDelay();
            messageVoNum++;
        }

        long messageAvgDelay = (messageVoNum == 0) ? 0 : messageTotalDelay / messageVoNum;

        inlongAuditMetricsManager.updateMessageNum(messageNum);
        inlongAuditMetricsManager.updateMessageSize(messageSize);
        inlongAuditMetricsManager.updateMessageAvgDelay(messageAvgDelay);
    }

    private void reportApiMetric(List<AuditDataVo> apiAuditDataVos){
        Long recNum = 0L;
        Long recSize = 0L;
        Long recTotalDelay = 0L;
        Long recVoNum=0L;
        Long sendNum = 0L;
        Long sendSize = 0L;
        Long sendTotalDelay = 0L;
        Long sendVoNum=0L;
        for (AuditDataVo auditDataVo : apiAuditDataVos){
            String auditId = auditDataVo.getAuditId();
            if ("1".equals(auditId)){
                recNum += auditDataVo.getCount();
                recSize += auditDataVo.getSize();
                recTotalDelay += auditDataVo.getDelay();
                recVoNum++;
            } else {
                sendNum += auditDataVo.getCount();
                sendSize += auditDataVo.getSize();
                sendTotalDelay += auditDataVo.getDelay();
                sendVoNum++;
            }
        }

        long recAvgDelay = (recVoNum == 0) ? 0 : recTotalDelay / recVoNum;
        long sendAvgDelay = (sendVoNum == 0) ? 0 : sendTotalDelay / sendVoNum;

        double abandonRate = (recNum == 0) ? 0.0 : (recNum - sendNum) * 1.0 / recNum;

        inlongApiMetricsManager.updateReceiveSuccessNum(recNum);
        inlongApiMetricsManager.updateReceiveSuccessSize(recSize);
        inlongApiMetricsManager.updateReceiveSuccessAvgDelay(recAvgDelay);
        inlongApiMetricsManager.updateSendSuccessNum(sendNum);
        inlongApiMetricsManager.updateSendSuccessSize(sendSize);
        inlongApiMetricsManager.updateSendSuccessAvgDelay(sendAvgDelay);
        inlongApiMetricsManager.updateAbandonRate(abandonRate);
    }

    private void reportAgentMetric(List<AuditDataVo> agentAuditDataVos) {
        Long recNum = 0L;
        Long recSize = 0L;
        Long recVoNum=0L;
        Long recTotalDelay=0L;
        Long sendNum = 0L;
        Long sendSize = 0L;
        Long sendVoNum=0L;
        Long sendTotalDelay=0L;
        for (AuditDataVo auditDataVo : agentAuditDataVos) {
            String auditId = auditDataVo.getAuditId();
            if ("3".equals(auditId)) {
                recNum += auditDataVo.getCount();
                recSize += auditDataVo.getSize();
                recTotalDelay+=auditDataVo.getDelay();
                recVoNum++;
            } else {
                sendNum += auditDataVo.getCount();
                sendSize += auditDataVo.getSize();
                sendTotalDelay+=auditDataVo.getDelay();
                sendVoNum++;
            }
        }
        long recAvgDelay = (recVoNum == 0) ? 0 : recTotalDelay / recVoNum;
        long sendAvgDelay = (sendVoNum == 0) ? 0 : sendTotalDelay / sendVoNum;
        double abandonRate = (recNum == 0) ? 0.0 : (recNum - sendNum) * 1.0 / recNum;
        long apiSendNum = inlongApiMetricsManager.getSendSuccessNum();
        double lossRate = (apiSendNum == 0) ? 0.0 : (apiSendNum - recNum) * 1.0 / apiSendNum;

        inlongAgentMetricsManager.updateReceiveSuccessNum(recNum);
        inlongAgentMetricsManager.updateReceiveSuccessSize(recSize);
        inlongAgentMetricsManager.updateReceiveSuccessAvgDelay(recAvgDelay);
        inlongAgentMetricsManager.updateSendSuccessNum(sendNum);
        inlongAgentMetricsManager.updateSendSuccessSize(sendSize);
        inlongAgentMetricsManager.updateSendSuccessAvgDelay(sendAvgDelay);
        inlongAgentMetricsManager.updateAbandonRate(abandonRate);
        inlongAgentMetricsManager.updateLossRate(lossRate);
    }

    private void reportDataproxyMetric(List<AuditDataVo> dataproxyAuditDataVos) {
        Long recNum = 0L;
        Long recSize = 0L;
        Long recTotalDelay = 0L;
        Long recVoNum = 0L;
        Long sendNum = 0L;
        Long sendSize = 0L;
        Long sendTotalDelay = 0L;
        Long sendVoNum = 0L;

        for (AuditDataVo auditDataVo : dataproxyAuditDataVos) {
            String auditId = auditDataVo.getAuditId();
            if ("5".equals(auditId)) {
                recNum += auditDataVo.getCount();
                recSize += auditDataVo.getSize();
                recTotalDelay += auditDataVo.getDelay();
                recVoNum++;
            } else {
                sendNum += auditDataVo.getCount();
                sendSize += auditDataVo.getSize();
                sendTotalDelay += auditDataVo.getDelay();
                sendVoNum++;
            }
        }

        long recAvgDelay = (recVoNum == 0) ? 0 : recTotalDelay / recVoNum;
        long sendAvgDelay = (sendVoNum == 0) ? 0 : sendTotalDelay / sendVoNum;

        double abandonRate = (recNum == 0) ? 0.0 : (recNum - sendNum) * 1.0 / recNum;

        long agentSendNum = inlongAgentMetricsManager.getSendSuccessNum();

        double lossRate = (agentSendNum == 0) ? 0.0 : (agentSendNum - recNum) * 1.0 / agentSendNum;

        inlongDataproxyMetricsManager.updateReceiveSuccessNum(recNum);
        inlongDataproxyMetricsManager.updateReceiveSuccessSize(recSize);
        inlongDataproxyMetricsManager.updateReceiveSuccessAvgDelay(recAvgDelay);
        inlongDataproxyMetricsManager.updateSendSuccessNum(sendNum);
        inlongDataproxyMetricsManager.updateSendSuccessSize(sendSize);
        inlongDataproxyMetricsManager.updateSendSuccessAvgDelay(sendAvgDelay);
        inlongDataproxyMetricsManager.updateAbandonRate(abandonRate);
        inlongDataproxyMetricsManager.updateLossRate(lossRate);
    }

    private List<AuditDataVo> generateTestAuditDataVo(int dataCount, double apiDiscardRate, double apiToAgentLossRate,
                                                     double agentDiscardRate, double agentToDataProxyLossRate,
                                                     double dataProxyDiscardRate) {
        List<AuditDataVo> result = new ArrayList<>();
        Random random = new Random();
        String groupId = "testGroupId";
        String streamId = "testStreamId";
        String ip = "127.0.0.1";
        String dockerId = "testDockerId";
        String threadId = "testThreadId";

        for (int i = 0; i < dataCount; i++) {
            long count = 20 + random.nextInt(181);
            long size = 1 + random.nextInt(1024);
            Timestamp logTs = new Timestamp(System.currentTimeMillis() - random.nextInt(3600000));

            AuditDataVo apiReceive = createAuditDataVo(ip, dockerId, threadId, logTs, groupId, streamId, "1", count, size, random.nextInt(201));
            result.add(apiReceive);

            if (random.nextDouble() >= apiDiscardRate) {
                AuditDataVo apiSend = createAuditDataVo(ip, dockerId, threadId, logTs, groupId, streamId, "2", count, size, random.nextInt(201));
                result.add(apiSend);

                if (random.nextDouble() >= apiToAgentLossRate) {
                    AuditDataVo agentReceive = createAuditDataVo(ip, dockerId, threadId, logTs, groupId, streamId, "3", count, size, random.nextInt(201));
                    result.add(agentReceive);

                    if (random.nextDouble() >= agentDiscardRate) {
                        AuditDataVo agentSend = createAuditDataVo(ip, dockerId, threadId, logTs, groupId, streamId, "4", count, size, random.nextInt(201));
                        result.add(agentSend);

                        if (random.nextDouble() >= agentToDataProxyLossRate) {
                            AuditDataVo dataProxyReceive = createAuditDataVo(ip, dockerId, threadId, logTs, groupId, streamId, "5", count, size, random.nextInt(201));
                            result.add(dataProxyReceive);

                            if (random.nextDouble() >= dataProxyDiscardRate) {
                                AuditDataVo dataProxySend = createAuditDataVo(ip, dockerId, threadId, logTs, groupId, streamId, "6", count, size, random.nextInt(201));
                                result.add(dataProxySend);
                            }
                        }
                    }
                }
            }
        }
        return result;
    }
    private AuditDataVo createAuditDataVo(String ip, String dockerId, String threadId, Timestamp logTs,
                                          String groupId, String streamId, String auditId,
                                          long count, long size, long delay) {
        AuditDataVo vo = new AuditDataVo();
        vo.setIp(ip);
        vo.setDockerId(dockerId);
        vo.setThreadId(threadId);
        vo.setLogTs(logTs);
        vo.setInlongGroupId(groupId);
        vo.setInlongStreamId(streamId);
        vo.setAuditId(auditId);
        vo.setCount(count);
        vo.setSize(size);
        vo.setDelay(delay);
        return vo;
    }


}
