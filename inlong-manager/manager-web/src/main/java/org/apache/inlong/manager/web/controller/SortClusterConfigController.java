package org.apache.inlong.manager.web.controller;

import com.google.gson.Gson;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.inlong.manager.common.pojo.sort.SortClusterConfigResponse;
import org.apache.inlong.manager.service.core.SortClusterConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/sort/standalone")
@Api(tags = "Sort Cluster Config")
public class SortClusterConfigController {

    @Autowired
    private SortClusterConfigService clusterConfigService;

    @PostMapping("/getClusterConfig")
    @ApiOperation("Get sort stand-alone cluster config")
    public String getClusterConfig(
            @RequestParam("clusterName") String clusterName,
            @RequestParam("md5") String md5,
            @RequestParam("apiVersion") String apiVersion) {
        Gson gson = new Gson();
        SortClusterConfigResponse response = clusterConfigService.get(clusterName, md5);
        String jsonObject = gson.toJson(response);
        return jsonObject;
    }

}
