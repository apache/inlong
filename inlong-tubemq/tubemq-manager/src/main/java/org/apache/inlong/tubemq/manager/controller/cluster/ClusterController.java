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

package org.apache.inlong.tubemq.manager.controller.cluster;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.tubemq.manager.controller.TubeMQResult;
import org.apache.inlong.tubemq.manager.controller.cluster.dto.ClusterDto;
import org.apache.inlong.tubemq.manager.controller.cluster.request.AddClusterReq;
import org.apache.inlong.tubemq.manager.controller.cluster.request.DeleteClusterReq;
import org.apache.inlong.tubemq.manager.controller.cluster.vo.ClusterVo;
import org.apache.inlong.tubemq.manager.entry.ClusterEntry;
import org.apache.inlong.tubemq.manager.entry.MasterEntry;
import org.apache.inlong.tubemq.manager.service.TubeConst;
import org.apache.inlong.tubemq.manager.service.TubeMQErrorConst;
import org.apache.inlong.tubemq.manager.service.interfaces.ClusterService;
import org.apache.inlong.tubemq.manager.service.interfaces.MasterService;
import org.apache.inlong.tubemq.manager.utils.ConvertUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1/cluster")
@Slf4j
public class ClusterController {

    private final Gson gson = new Gson();

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private MasterService masterService;

    @PostMapping("")
    public @ResponseBody
        TubeMQResult clusterMethodProxy(@RequestParam String method, @RequestBody String req) {
        switch (method) {
            case TubeConst.ADD:
                return addNewCluster(gson.fromJson(req, AddClusterReq.class));
            case TubeConst.DELETE:
                return deleteCluster(gson.fromJson(req, DeleteClusterReq.class));
            case TubeConst.MODIFY:
                return changeCluster(gson.fromJson(req, ClusterDto.class));
            default:
                return TubeMQResult.errorResult(TubeMQErrorConst.NO_SUCH_METHOD);
        }
    }

    /**
     * change cluster info
     *
     * @param clusterDto
     * @return
     */
    private TubeMQResult changeCluster(ClusterDto clusterDto) {
        if (!clusterDto.legal()) {
            return TubeMQResult.errorResult(TubeMQErrorConst.PARAM_ILLEGAL);
        }
        return clusterService.modifyCluster(clusterDto);
    }

    /**
     * add a new cluster, should provide a master node
     */
    public TubeMQResult addNewCluster(AddClusterReq req) {
        // 1. validate params
        if (!req.legal()) {
            return TubeMQResult.errorResult(TubeMQErrorConst.PARAM_ILLEGAL);
        }
        TubeMQResult checkResult = masterService.checkMasterNodeStatus(req.getMasterIp(), req.getMasterWebPort());
        if (checkResult.getErrCode() != TubeConst.SUCCESS_CODE) {
            return TubeMQResult.errorResult("please check master ip and webPort");
        }
        // 2. add cluster and master node
        clusterService.addClusterAndMasterNode(req);
        return new TubeMQResult();
    }

    /**
     * query cluster info, if no clusterId is passed, return all clusters
     *
     * @param clusterId
     * @return
     */
    @RequestMapping(value = "", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public TubeMQResult queryCluster(@RequestParam(required = false) Integer clusterId) {
        // return all clusters if no clusterId passed
        if (clusterId == null) {
            return queryAllClusterVo();
        }

        ClusterEntry clusterEntry = clusterService.getOneCluster(clusterId);
        if (clusterEntry == null) {
            return TubeMQResult.errorResult("no such cluster with id " + clusterId);
        }

        MasterEntry masterNode = masterService.getMasterNode(clusterEntry.getClusterId());

        TubeMQResult result = new TubeMQResult();
        result.setData(Lists.newArrayList(ConvertUtils.convertToClusterVo(clusterEntry, masterNode)));
        return result;
    }

    /**
     * get all cluster info
     *
     * @return
     */
    private TubeMQResult queryAllClusterVo() {
        TubeMQResult result = new TubeMQResult();
        List<ClusterEntry> allClusters = clusterService.getAllClusters();
        List<ClusterVo> clusterVos = Lists.newArrayList();
        for (ClusterEntry cluster : allClusters) {
            MasterEntry masterNode = masterService.getMasterNode(cluster.getClusterId());
            ClusterVo clusterVo = ConvertUtils.convertToClusterVo(cluster, masterNode);
            clusterVos.add(clusterVo);
        }
        result.setData(clusterVos);
        return result;
    }

    /**
     * delete a new cluster
     */
    public TubeMQResult deleteCluster(DeleteClusterReq req) {
        // 1. validate params
        if (req.getClusterId() == null || StringUtils.isEmpty(req.getModifyUser())) {
            return TubeMQResult.errorResult("please input clusterId and modifyUser");
        }
        // 2. delete cluster
        clusterService.deleteCluster(req.getClusterId());
        return new TubeMQResult();
    }


    /**
     * query cluster info
     */
    @RequestMapping(value = "/query", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
        String queryInfo(
            @RequestParam Map<String, String> queryBody) throws Exception {
        String url = masterService.getQueryUrl(queryBody);
        return masterService.queryMaster(url);
    }

}
