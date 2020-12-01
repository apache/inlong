package org.apache.tubemq.manager.controller.node;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.controller.TubeMQResult;
import org.apache.tubemq.manager.service.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/v1/node")
@Slf4j
public class NodeController {

    public static final String NO_SUCH_METHOD = "no such method";
    public static final String OP_QUERY = "op_query";
    public static final String ADMIN_QUERY_CLUSTER_INFO = "admin_query_cluster_info";

    private final Gson gson = new Gson();

    @Autowired
    NodeService nodeService;

    @RequestMapping(value = "/query", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody String queryInfo(@RequestParam String type, @RequestParam String method,
            @RequestParam(required = false) Integer clusterId) {

        if (method.equals(ADMIN_QUERY_CLUSTER_INFO) && type.equals(OP_QUERY)) {
            return nodeService.queryClusterInfo(clusterId);
        }

        return gson.toJson(TubeMQResult.getErrorResult(NO_SUCH_METHOD));
    }

}
