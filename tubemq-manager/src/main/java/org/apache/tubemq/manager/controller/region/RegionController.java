/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.controller.region;

import static org.apache.tubemq.manager.service.TubeMQErrorConst.PARAM_ILLEGAL;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.ADD;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.DELETE;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.MODIFY;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.NO_SUCH_CLUSTER;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.NO_SUCH_METHOD;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.QUERY;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.entry.ClusterEntry;
import org.apache.tubemq.manager.entry.RegionEntry;
import org.apache.tubemq.manager.repository.NodeRepository;
import org.apache.tubemq.manager.service.interfaces.ClusterService;
import org.apache.tubemq.manager.service.interfaces.MasterService;
import org.apache.tubemq.manager.service.interfaces.NodeService;
import org.apache.tubemq.manager.service.interfaces.RegionService;
import org.apache.tubemq.manager.utils.ValidateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/v1/region")
@Slf4j
public class RegionController {
    private final Gson gson = new GsonBuilder().serializeNulls().create();

    @Autowired
    NodeService nodeService;

    @Autowired
    NodeRepository nodeRepository;

    @Autowired
    MasterService masterService;

    @Autowired
    RegionService regionService;

    @Autowired
    ClusterService clusterService;

    /**
     * broker method proxy
     * divides the operation on broker to different method
     */
    @RequestMapping(value = "")
    public @ResponseBody
    TubeMQResult brokerMethodProxy(
        @RequestParam String method, @RequestBody String req) {
        switch (method) {
            case ADD:
                return createNewRegion(gson.fromJson(req, CreateRegionReq.class));
            case DELETE:
                return deleteRegion(gson.fromJson(req, DeleteRegionReq.class));
            case MODIFY:
                return modifyRegion(gson.fromJson(req, ModifyRegionReq.class));
            case QUERY:
                return queryRegion(gson.fromJson(req, QueryRegionReq.class));
                default:
                return TubeMQResult.errorResult(NO_SUCH_METHOD);
        }
    }

    private TubeMQResult queryRegion(QueryRegionReq req) {
        if (ValidateUtils.isNull(req.getClusterId())) {
            return TubeMQResult.errorResult(PARAM_ILLEGAL);
        }

        List<RegionEntry> regionEntries = regionService
            .queryRegion(req.getRegionId(), req.getClusterId());

        return TubeMQResult.successResult(regionEntries);
    }

    private TubeMQResult deleteRegion(DeleteRegionReq req) {
        return regionService.deleteRegion(req.getRegionId(), req.getClusterId());
    }

    private TubeMQResult createNewRegion(CreateRegionReq req) {
        RegionEntry regionEntry = req.getRegionEntry();
        if (ValidateUtils.isNull(regionEntry) || !regionEntry.legal() ||
            ValidateUtils.isNull(req.getBrokerIdList())) {
            return TubeMQResult.errorResult(PARAM_ILLEGAL);
        }
        ClusterEntry clusterEntry = clusterService.getOneCluster(
            req.getClusterId());
        if (clusterEntry == null) {
            return TubeMQResult.errorResult(NO_SUCH_CLUSTER);
        }
        return regionService.createNewRegion(regionEntry, req.getBrokerIdList());
    }

    private TubeMQResult modifyRegion(ModifyRegionReq req) {
        RegionEntry regionEntry = req.getRegionEntry();
        if (!regionEntry.legal() || ValidateUtils.isNull(regionEntry.getClusterId())) {
            return TubeMQResult.errorResult(PARAM_ILLEGAL);
        }
        return regionService.updateRegion(req.getRegionEntry(),
            req.getBrokerIdList(), req.getClusterId());
    }


}
