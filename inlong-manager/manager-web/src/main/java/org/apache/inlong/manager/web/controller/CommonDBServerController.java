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

package org.apache.inlong.manager.web.controller;

import com.github.pagehelper.PageInfo;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerInfo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerListVo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerPageRequest;
import org.apache.inlong.manager.common.util.LoginUserUtil;
import org.apache.inlong.manager.common.util.SmallTools;
import org.apache.inlong.manager.service.core.CommonDBServerService;
import org.apache.inlong.manager.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ClassUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("commonserver/db")
@Api(tags = "Common Server - DB")
public class CommonDBServerController {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonDBServerController.class);

    @Autowired
    CommonDBServerService commonDBServerService;

    @RequestMapping(value = "/create", method = RequestMethod.POST)
    @ApiOperation(value = "Create DB data source")
    public Response<Integer> create(@RequestBody CommonDbServerInfo commonDbServerInfo) throws Exception {
        int id = commonDBServerService.save(commonDbServerInfo);
        return Response.success(id);
    }

    @RequestMapping(value = "/getById/{id}", method = RequestMethod.GET)
    @ApiOperation(value = "Get DB data source")
    public Response<CommonDbServerInfo> get(@PathVariable int id) throws Exception {
        CommonDbServerInfo result = commonDBServerService.get(id);
        return Response.success(result);
    }

    @RequestMapping(value = "/deleteById/{id}", method = RequestMethod.DELETE)
    @ApiOperation(value = "Delete DB data source")
    public Response<CommonDbServerInfo> delete(@PathVariable int id) throws Exception {
        commonDBServerService.delete(id);
        return Response.success();
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    @ApiOperation(value = "Modify DB data source")
    public Response<CommonDbServerInfo> update(@RequestBody CommonDbServerInfo commonDbServerInfo) throws Exception {
        CommonDbServerInfo result = commonDBServerService.update(commonDbServerInfo);
        return Response.success(result);
    }

    @RequestMapping(value = "/getByUser/{username}", method = RequestMethod.GET)
    @ApiOperation(value = "Get DB data source by user")
    @ApiImplicitParam(name = "username", dataTypeClass = String.class, required = true)
    public Response<List<CommonDbServerInfo>> getByUser(@PathVariable String username) throws Exception {
        List<CommonDbServerInfo> result = commonDBServerService.getByUser(username);
        return Response.success(result);
    }

    @PostMapping("/addVisiblePerson/{id}")
    @ApiOperation(value = "Add visible Person")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "visiblePerson", value = "Visible person list, separated by commas",
                    dataTypeClass = String.class)
    })
    public Response<CommonDbServerInfo> addVisiblePerson(
            @PathVariable("id") Integer id, @RequestParam("visiblePerson") String visiblePerson) {
        CommonDbServerInfo entity = commonDBServerService.addVisiblePerson(id, visiblePerson);
        return Response.success(entity);
    }

    @PostMapping("/deleteVisiblePerson/{id}")
    @ApiOperation(value = "Delete visible Person")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "visiblePerson", value = "Visible person list, separated by commas",
                    dataTypeClass = String.class)
    })
    public Response<CommonDbServerInfo> deleteVisiblePerson(
            @PathVariable("id") Integer id, @RequestParam("visiblePerson") String visiblePerson) {
        CommonDbServerInfo entity = commonDBServerService.deleteVisiblePerson(id, visiblePerson);
        return Response.success(entity);
    }

    @PostMapping("/addVisibleGroup/{id}")
    @ApiOperation(value = "Add visible group")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "visibleGroup", value = "Visible group list, separated by commas",
                    dataTypeClass = String.class)
    })
    public Response<CommonDbServerInfo> addVisibleGroup(
            @PathVariable("id") Integer id, @RequestParam("visibleGroup") String visibleGroup) {
        CommonDbServerInfo entity = commonDBServerService.addVisibleGroup(id, visibleGroup);
        return Response.success(entity);
    }

    @PostMapping("/deleteVisibleGroup/{id}")
    @ApiOperation(value = "Delete visible group")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "id", dataTypeClass = Integer.class),
            @ApiImplicitParam(name = "visibleGroup", value = "Visible group list, separated by commas",
                    dataTypeClass = String.class)
    })
    public Response<CommonDbServerInfo> deleteVisibleGroup(
            @PathVariable("id") Integer id, @RequestParam("visibleGroup") String visibleGroup) {
        CommonDbServerInfo entity = commonDBServerService.deleteVisibleGroup(id, visibleGroup);
        return Response.success(entity);
    }

    @RequestMapping(value = "/list", method = RequestMethod.POST)
    @ApiOperation(value = "Query data source list based on conditions")
    public Response<PageInfo<CommonDbServerListVo>> listByCondition(CommonDbServerPageRequest request)
            throws Exception {
        return Response.success(commonDBServerService.listByCondition(request));
    }

    /**
     * Download import template
     */
    @RequestMapping(value = "/download", method = RequestMethod.GET)
    @ApiOperation(value = "Download import template")
    public String download(HttpServletResponse response) {
        String fileName = "common_db_server_template.csv";
        String realPath = new File("").getAbsolutePath();
        File file = new File(realPath, fileName);
        if (file.exists()) {
            try (FileInputStream fis = new FileInputStream(file);
                    BufferedInputStream bis = new BufferedInputStream(fis)) {
                response.setContentType("application/force-download");
                // Set file name Support Chinese
                fileName = URLEncoder.encode(fileName, "UTF-8");
                response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);

                byte[] buffer = new byte[1024];
                OutputStream os = response.getOutputStream();
                int i = bis.read(buffer);
                while (i != -1) {
                    os.write(buffer, 0, i);
                    i = bis.read(buffer);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Batch import - upload import config
     */
    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    @ApiOperation(value = "Batch Import - upload config")
    public String upload(@RequestParam("file") MultipartFile file) throws Exception {
        Preconditions.checkFalse(file.isEmpty(), "File cannot be empty");
        String fileName = file.getOriginalFilename();
        LOGGER.info("The name of the uploaded file is: " + fileName);

        String filePath = ClassUtils.getDefaultClassLoader().getResource("").getPath();
        File fileDir = new File(filePath, "common_server");
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }

        String username = LoginUserUtil.getLoginUserDetail().getUserName();
        String time = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        fileName = username + "_" + time + "_" + fileName;

        Path path = Paths.get(fileDir.getAbsolutePath(), fileName);
        if (Files.exists(path)) {
            throw new BusinessException(BizErrorCodeEnum.COMMON_FILE_UPLOAD_FAIL,
                    "The file [" + fileName + "] already exists, please try again later");
        }

        int count = 0;
        try {
            // Save files
            file.transferTo(path.toFile());

            try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                ColumnPositionMappingStrategy strategy = new ColumnPositionMappingStrategy();
                strategy.setType(CommonDbServerInfo.class);
                String[] fields = {"connectionName", "dbType", "dbServerIp", "port", "dbName", "username",
                        "password", "hasSelect", "hasInsert", "hasUpdate", "hasDelete", "inCharges", "isRegionId",
                        "dbDescription", "backupDbServerIp", "backupDbPort", "visiblePerson", "visibleGroup"};
                strategy.setColumnMapping(fields);

                CsvToBean csvToBean = new CsvToBeanBuilder(br)
                        .withType(CommonDbServerInfo.class)
                        .withMappingStrategy(strategy)
                        .withIgnoreLeadingWhiteSpace(true)
                        .withSeparator(',')
                        .build();

                List<CommonDbServerInfo> dbServerInfos = csvToBean.parse();
                if (dbServerInfos.size() > 10000) {
                    return "Failed, the number of data exceeds the upper limit [10000]";
                }

                StringBuilder sb = new StringBuilder();
                sb.append("csv format, The fields are connectionName, dbType, dbServerIp, port, dbName, "
                        + "username, password, hasSelect, hasInsert, hasUpdate, hasDelete, inCharges, "
                        + "isRegionId, dbDescription, backupDbServerIp, backupDbPort, visiblePerson, visibleGroup\n");
                sb.append("Incorrect data check\n");
                // check
                int i = 1;
                boolean passed = true;
                for (CommonDbServerInfo entry : dbServerInfos) {
                    if (!SmallTools.ipCheck(entry.getDbServerIp())) {
                        sb.append(i + " column, dbServerIp=[" + entry.getDbServerIp() + "]Incorrect check\n");
                        passed = false;
                    }
                    if (!SmallTools.portCheck(entry.getPort())) {
                        sb.append(i + " column, port=[" + entry.getPort() + "]Incorrect check\n");
                        passed = false;
                    }
                    i++;
                }
                if (!passed) {
                    return sb.toString();
                }

                for (CommonDbServerInfo entry : dbServerInfos) {
                    commonDBServerService.save(entry);
                    //System.out.println("entry = "+entry);
                    count++;
                }
            }

            return "Overall success, a total of 2 " + count + "are written.";
        } catch (IllegalStateException | IOException e) {
            e.printStackTrace();
        }
        return "Overall failure, a total of 2 " + count + "are written.";
    }
}
