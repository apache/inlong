/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.config.pojo;

/**
 * 
 * SortClusterResponse
 */
public class SortClusterResponse {

    public static final int SUCC = 0;
    public static final int NOUPDATE = 1;
    public static final int FAIL = -1;
    public static final int REQ_PARAMS_ERROR = -101;

    private boolean result;
    private int errCode;
    private String md5;
    private SortClusterConfig data;

    /**
     * get result
     * 
     * @return the result
     */
    public boolean isResult() {
        return result;
    }

    /**
     * set result
     * 
     * @param result the result to set
     */
    public void setResult(boolean result) {
        this.result = result;
    }

    /**
     * get errCode
     * 
     * @return the errCode
     */
    public int getErrCode() {
        return errCode;
    }

    /**
     * set errCode
     * 
     * @param errCode the errCode to set
     */
    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    /**
     * get md5
     * 
     * @return the md5
     */
    public String getMd5() {
        return md5;
    }

    /**
     * set md5
     * 
     * @param md5 the md5 to set
     */
    public void setMd5(String md5) {
        this.md5 = md5;
    }

    /**
     * get data
     * 
     * @return the data
     */
    public SortClusterConfig getData() {
        return data;
    }

    /**
     * set data
     * 
     * @param data the data to set
     */
    public void setData(SortClusterConfig data) {
        this.data = data;
    }
//
//    /**
//     * generateTdbankConfig
//     * 
//     * @return
//     */
//    public static SortClusterConfig generateTdbankConfig() {
//        SortClusterConfig clusterConfig = new SortClusterConfig();
//        clusterConfig.setClusterName("tdbankv3-sz-sz1");
//        //
//        List<SortTaskConfig> sortTasks = new ArrayList<>();
//        clusterConfig.setSortTasks(sortTasks);
//        SortTaskConfig taskConfig = new SortTaskConfig();
//        sortTasks.add(taskConfig);
//        taskConfig.setName("sid_tdbank_atta6th_v3");
//        taskConfig.setType(SortType.TQTDBANK);
//        //
//        Map<String, String> sinkParams = new HashMap<>();
//        taskConfig.setSinkParams(sinkParams);
//        sinkParams.put("b_pcg_venus_szrecone_124_153_utf8", "10.56.15.195:46801,10.56.15.212:46801,"
//                + "10.56.15.220:46801,10.56.15.221:46801,"
//                + "10.56.15.230:46801,10.56.16.20:46801,10.56.16.38:46801,10.56.20.21:46801,10.56.20.80:46801,"
//                + "10.56.20.85:46801,10.56.209.205:46801,10.56.21.17:46801,10.56.21.20:46801,10.56.21.79:46801,"
//                + "10.56.21.85:46801,10.56.81.205:46801,10.56.81.211:46801,10.56.82.11:46801,10.56.82.12:46801,"
//                + "10.56.82.37:46801,10.56.82.38:46801,10.56.82.40:46801,10.56.83.143:46801,10.56.83.80:46801,"
//                + "10.56.84.17:46801");
//        //
//        List<Map<String, String>> idParams = new ArrayList<>();
//        Map<String, String> idParam = new HashMap<>();
//        idParams.add(idParam);
//        taskConfig.setIdParams(idParams);
//        idParam.put(Constants.INLONG_GROUP_ID, "0fc00000046");
//        idParam.put(Constants.INLONG_STREAM_ID, "");
//        idParam.put(TdbankConfig.KEY_BID, "b_pcg_venus_szrecone_124_153_utf8");
//        idParam.put(TdbankConfig.KEY_TID, "t_sh_atta_v2_0fc00000046");
//        idParam.put(TdbankConfig.KEY_DATA_TYPE, TdbankConfig.DATA_TYPE_ATTA_TEXT);
//        return clusterConfig;
//    }
//
//    /**
//     * generateCdmqConfig
//     * 
//     * @return
//     */
//    public static SortClusterConfig generateCdmqConfig() {
//        SortClusterConfig clusterConfig = new SortClusterConfig();
//        clusterConfig.setClusterName("cdmqv3-sz-sz1");
//        //
//        List<SortTaskConfig> sortTasks = new ArrayList<>();
//        clusterConfig.setSortTasks(sortTasks);
//        SortTaskConfig taskConfig = new SortTaskConfig();
//        sortTasks.add(taskConfig);
//        taskConfig.setName("sid_cdmq_kg_videorequest_v3");
//        taskConfig.setType(SortType.CDMQ);
//        //
//        Map<String, String> sinkParams = new HashMap<>();
//        taskConfig.setSinkParams(sinkParams);
//        sinkParams.put("cdmqAccessPoint", "cdmqszentry01.data.mig:10005,cdmqszentry05.data.mig:10033");
//        sinkParams.put("cdmqClusterId", "kg_videorequest");
//        sinkParams.put("clientId", "p_video_atta_196");
//        sinkParams.put("batchSize", "122880");
//        sinkParams.put("maxRequestSize", "8388608");
//        sinkParams.put("lingerMs", "150");
//        //
//        List<Map<String, String>> idParams = new ArrayList<>();
//        Map<String, String> idParam = new HashMap<>();
//        idParams.add(idParam);
//        taskConfig.setIdParams(idParams);
//        idParam.put(Constants.INLONG_GROUP_ID, "0fc00000046");
//        idParam.put(Constants.TOPIC, "U_TOPIC_0fc00000046");
//        return clusterConfig;
//    }
//
//    /**
//     * main
//     * 
//     * @param args
//     */
//    public static void main(String[] args) {
//        // tdbank
//        {
//            SortClusterConfig config = generateTdbankConfig();
//            String configString = JSON.toJSONString(config, false);
//            System.out.println("tdbank:" + configString);
//            String md5 = DigestUtils.md5Hex(configString);
//            SortClusterResponse response = new SortClusterResponse();
//            response.setResult(true);
//            response.setErrCode(SUCC);
//            response.setMd5(md5);
//            response.setData(config);
//            String responseString = JSON.toJSONString(response, true);
//            System.out.println("tdbank responseString:" + responseString);
//        }
//        // cdmq
//        {
//            SortClusterConfig config = generateCdmqConfig();
//            String configString = JSON.toJSONString(config, false);
//            System.out.println("cdmq:" + configString);
//            String md5 = DigestUtils.md5Hex(configString);
//            SortClusterResponse response = new SortClusterResponse();
//            response.setResult(true);
//            response.setErrCode(SUCC);
//            response.setMd5(md5);
//            response.setData(config);
//            String responseString = JSON.toJSONString(response, true);
//            System.out.println("cdmq responseString:" + responseString);
//        }
//    }
}
